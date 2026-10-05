//! Small models of broadcast protocols.
//! Cursor assertions check cell protection without tracking payload accesses.

use super::*;
use crate::sync::atomic::AtomicUsize;
use loom::thread;

const CAPACITY: usize = 1;

fn broadcast() -> Broadcast<usize> {
    let config = BroadcastConfig {
        capacity: CAPACITY,
        producer_slots: 1,
        consumer_slots: 1,
    };
    let layout = QueueLayout::new::<usize>(&config).unwrap();
    let region = Region::alloc(NonZeroUsize::new(layout.total).unwrap()).unwrap();
    // SAFETY: fresh heap allocation, initialized once with this queue layout.
    let queue = unsafe { SharedQueue::create_in_region::<usize>(&region, &config, 0) }.unwrap();
    Broadcast::from_queue(queue)
}

// Tests the publication-based double-sample join handshake. A racing consumer
// must either start past earlier publications or install a limit that prevents
// the producer from overwriting its unread cell, including while a guard is held.
#[test]
fn joining_consumer_protects_unread_cell() {
    loom::model(|| {
        let broadcast = broadcast();
        let mut producer = broadcast.producer().unwrap();
        let writer = thread::spawn(move || {
            // The first write establishes a frontier while the consumer joins.
            assert!(producer.try_write(0).is_ok());
            // Capacity one: a second write would reuse the same physical cell.
            let _ = producer.try_write(1);
        });

        // Join while the producer can publish either value.
        let mut consumer = broadcast.consumer().unwrap();
        let next = consumer.core.next_for_lane(0);
        // If data is available, hold the guard until writing finishes.
        // Do not dereference the payload: cursor checks detect an overwrite
        // without introducing a data race if the protocol is broken.
        let guard = consumer.try_reserve_read();
        writer.join().unwrap();

        // Joining must either skip earlier publications or constrain the writer.
        // Completing the writer makes this sample observe its final cursor.
        let lane = broadcast.shared_queue.producer_lanes().next().unwrap();
        let published = lane.published();
        assert!(published >= next, "publication precedes the join position");
        assert!(
            published - next <= CAPACITY,
            "producer overwrote an unread cell"
        );

        // Only dropping the guard may release the cell through consumer progress.
        drop(guard);
    });
}

// Tests the release/acquire producer ownership handover. A replacement that
// acquires a released lane must observe the previous owner's publication
// cursor and respect a racing consumer's join limit. Ownership
// transfer must preserve the ordering required by the join handshake.
#[test]
fn replacement_producer_respects_joining_consumer() {
    loom::model(|| {
        let broadcast = broadcast();
        let mut producer = broadcast.producer().unwrap();
        let writer = thread::spawn(move || {
            assert!(producer.try_write(0).is_ok());
            // Release ownership after completing publication.
            drop(producer);
        });

        let replacement_broadcast = broadcast.clone();
        let replacement = thread::spawn(move || {
            // Failure means the old owner is live. Successful acquisition must
            // observe its completed write before checking the consumer's limit.
            if let Ok(mut replacement) = replacement_broadcast.producer() {
                assert_eq!(replacement.lane.published(), 1);
                // Refuse this write if the joining consumer pins the only cell.
                if replacement.try_write(1).is_ok() {
                    assert_eq!(replacement.lane.published(), 2);
                }
            }
        });

        let mut consumer = broadcast.consumer().unwrap();
        let next = consumer.core.next_for_lane(0);
        let guard = consumer.try_reserve_read();
        writer.join().unwrap();
        replacement.join().unwrap();

        // Neither owner may advance past the cell protected by the consumer.
        let lane = broadcast.shared_queue.producer_lanes().next().unwrap();
        let published = lane.published();
        assert!(published >= next, "publication precedes the join position");
        assert!(
            published - next <= CAPACITY,
            "replacement overwrote an unread cell"
        );
        drop(guard);
    });
}

// Tests the waiter registration and broadcast wake-counter protocol. Publication
// racing with registration, the condition recheck, or compare-and-sleep must
// either be observed before sleeping or wake the consumer; no wake may be lost.
#[test]
fn publication_wakes_waiter() {
    loom::model(|| {
        let waiters = Arc::new(Waiters::default());
        let published = Arc::new(AtomicUsize::new(0));
        let wake_word = Arc::new(AtomicUsize::new(0));
        let consumer = {
            let waiters = waiters.clone();
            let published = published.clone();
            let wake_word = wake_word.clone();
            thread::spawn(move || {
                waiters
                    .wait_for(&wake_word, 0, Duration::MAX, || {
                        (published.load(Ordering::Acquire) == 1).then_some(())
                    })
                    .unwrap();
            })
        };

        published.store(1, Ordering::Release);
        waiters.bump_and_wake(&wake_word);
        consumer.join().unwrap();
    });
}
