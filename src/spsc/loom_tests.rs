//! Small models of the SPSC publication, release, and wait protocols.
//!
//! Raw buffer accesses are invisible to Loom. Shadow cells model payload reads
//! and writes inside the real queue's reservations, so missing synchronization
//! is detected without concurrently dereferencing untracked queue payloads.
//! Initialization uses shadow metadata; allocation and mapping are not modeled.
//! These bounded models do not cover timeouts or OS futex behavior.

use super::*;
use loom::{cell::UnsafeCell, sync::Arc, thread};

fn queue<T>(capacity: usize, position: usize) -> (Producer<T>, Consumer<T>) {
    let size = minimum_region_size::<T>(capacity);
    let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
    let header = region.addr().cast::<SharedQueueHeader>();
    // Zeroed memory does not construct atomic objects in Loom's runtime.
    let value = SharedQueueHeader {
        magic: AtomicU64::new(MAGIC),
        version: VERSION,
        buffer_mask: u32::try_from(capacity - 1).unwrap(),
        identifier: DEFAULT_QUEUE_IDENTIFIER,
        write: CacheAlignedAtomicSize::default(),
        read: CacheAlignedAtomicSize::default(),
        waiters: Waiters::default(),
    };
    value.write.store(position, Ordering::Relaxed);
    value.read.store(position, Ordering::Relaxed);
    // SAFETY: the fresh region is sized and aligned for this header and buffer.
    unsafe { header.write(value) };
    // SAFETY: the header was just initialized in this region.
    let producer = unsafe { Producer::from_header(region, header) }.unwrap();
    // SAFETY: this fresh queue has no other consumer.
    let consumer = unsafe { producer.join_as_consumer() }.unwrap();
    (producer, consumer)
}

fn write(batch: &mut WriteBatch<'_, usize>, cell: &UnsafeCell<usize>, value: usize) -> bool {
    if batch.try_write(0).is_err() {
        return false;
    }
    cell.with_mut(|ptr| {
        // SAFETY: a successful write reserves the corresponding physical slot
        // exclusively until the consumer releases it. Loom checks that ordering.
        unsafe { *ptr = value };
    });
    true
}

fn read(cell: &UnsafeCell<usize>) -> usize {
    cell.with(|ptr| {
        // SAFETY: callers hold the corresponding read reservation. Loom checks
        // that publication precedes this access and reuse follows its release.
        unsafe { *ptr }
    })
}

type Payloads = Arc<Vec<UnsafeCell<usize>>>;

fn queue_with_payloads(
    capacity: usize,
    position: usize,
) -> (Producer<usize>, Consumer<usize>, Payloads) {
    let (producer, consumer) = queue(capacity, position);
    let cells = Arc::new((0..capacity).map(|_| UnsafeCell::new(0)).collect());
    (producer, consumer, cells)
}

fn write_batch(producer: &mut Producer<usize>, cells: &[UnsafeCell<usize>], values: &[usize]) {
    let mut batch = producer.write_batch();
    for &value in values {
        let index = batch.producer.queue.mask(batch.producer.queue.cached_write);
        assert!(write(&mut batch, &cells[index], value));
    }
}

fn read_batch(
    consumer: &mut Consumer<usize>,
    cells: &[UnsafeCell<usize>],
    max: usize,
) -> Option<Vec<usize>> {
    let batch = consumer.try_reserve_read_batch(NonZeroUsize::new(max).unwrap())?;
    Some(
        (0..batch.len())
            .map(|offset| {
                let position = batch.reservation.start.wrapping_add(offset);
                let index = batch.reservation.consumer.queue.mask(position);
                read(&cells[index])
            })
            .collect(),
    )
}

#[test]
fn initialization_publishes_header_metadata() {
    loom::model(|| {
        // Raw metadata accesses are invisible to Loom. The shadow starts empty
        // and is filled before the real header publication operation.
        let metadata = Arc::new(UnsafeCell::new((0, 0, 0)));
        let header = Arc::new(SharedQueueHeader {
            magic: AtomicU64::new(0),
            version: VERSION,
            buffer_mask: 1,
            identifier: 42,
            write: CacheAlignedAtomicSize::default(),
            read: CacheAlignedAtomicSize::default(),
            waiters: Waiters::default(),
        });
        assert!(!header.is_initialized());
        let initializer_header = header.clone();
        let initializer_metadata = metadata.clone();
        let initializer = thread::spawn(move || {
            initializer_metadata.with_mut(|ptr| {
                // SAFETY: the sole initializer owns the metadata until it
                // publishes magic. Loom checks the joining reader's ordering.
                unsafe { *ptr = (VERSION, 1, 42) };
            });
            initializer_header.publish_initialization();
        });

        // The join may reject an unfinished header. Observing magic must make
        // all initialized metadata visible without joining the writer first.
        if header.is_initialized() {
            metadata.with(|ptr| {
                // SAFETY: successful initialization publication protects this
                // access. Loom detects missing release/acquire synchronization.
                let initialized = unsafe { *ptr };
                assert_eq!(
                    initialized,
                    (header.version, header.buffer_mask, header.identifier)
                );
            });
        }
        initializer.join().unwrap();
        assert!(header.is_initialized());
    });
}

#[test]
fn batch_publication_orders_payload() {
    loom::model(|| {
        // Both values fit contiguously, with no cursor or buffer wraparound.
        let (mut producer, mut consumer, cells) = queue_with_payloads(4, 0);
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            write_batch(&mut producer, &writer_cells, &[10, 11]);
        });

        // Race publication: the complete batch must be visible or nothing yet.
        let items = read_batch(&mut consumer, &cells, 2);
        writer.join().unwrap();
        let items = items
            .or_else(|| read_batch(&mut consumer, &cells, 2))
            .unwrap();
        assert_eq!(items, [10, 11]);
        assert_eq!(consumer.queue.cached_read, 2);
        assert!(read_batch(&mut consumer, &cells, 2).is_none());
    });
}

#[test]
fn batch_publication_orders_payload_across_cursor_wrap() {
    loom::model(|| {
        // The logical cursor advances from usize::MAX to zero.
        let (mut producer, mut consumer, cells) = queue_with_payloads(4, usize::MAX);
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            write_batch(&mut producer, &writer_cells, &[10, 11]);
        });

        // Race publication: the complete batch must be visible or nothing yet.
        let items = read_batch(&mut consumer, &cells, 2);
        writer.join().unwrap();
        let items = items
            .or_else(|| read_batch(&mut consumer, &cells, 2))
            .unwrap();
        assert_eq!(items, [10, 11]);
        assert_eq!(consumer.queue.cached_read, 1);
        assert!(read_batch(&mut consumer, &cells, 2).is_none());
    });
}

#[test]
fn batch_publication_orders_payload_across_buffer_wrap() {
    loom::model(|| {
        // Physical slots three and zero, while the logical cursor advances normally.
        let (mut producer, mut consumer, cells) = queue_with_payloads(4, 3);
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            write_batch(&mut producer, &writer_cells, &[10, 11]);
        });

        // Race publication: the complete batch must be visible or nothing yet.
        let items = read_batch(&mut consumer, &cells, 2);
        writer.join().unwrap();
        let items = items
            .or_else(|| read_batch(&mut consumer, &cells, 2))
            .unwrap();
        assert_eq!(items, [10, 11]);
        assert_eq!(consumer.queue.cached_read, 5);
        assert!(read_batch(&mut consumer, &cells, 2).is_none());
    });
}

#[test]
fn repeated_batches_preserve_fifo_during_slot_reuse() {
    loom::model(|| {
        let (mut producer, mut consumer, cells) = queue_with_payloads(2, 0);
        write_batch(&mut producer, &cells, &[10, 11]);
        assert_eq!(producer.try_write(0), Err(0));

        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            let mut written = 0;
            // Bounded attempts let Loom explore both full and reusable slots
            // without an unbounded retry loop.
            for _ in 0..2 {
                let mut batch = producer.write_batch();
                let index = batch.producer.queue.mask(batch.producer.queue.cached_write);
                if write(&mut batch, &writer_cells[index], 12 + written) {
                    written += 1;
                }
            }
            (producer, written)
        });

        let mut next = 10;
        for _ in 0..2 {
            if let Some(items) = read_batch(&mut consumer, &cells, 2) {
                for value in items {
                    assert_eq!(value, next);
                    next += 1;
                }
            }
        }
        let (_producer, written) = writer.join().unwrap();
        if let Some(items) = read_batch(&mut consumer, &cells, 2) {
            for value in items {
                assert_eq!(value, next);
                next += 1;
            }
        }
        assert_eq!(next, 12 + written);
        assert!(read_batch(&mut consumer, &cells, 2).is_none());
    });
}

#[test]
fn read_reservation_orders_slot_reuse_across_cursor_wrap() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue(1, usize::MAX);
        let cell = Arc::new(UnsafeCell::new(0));
        assert!(write(&mut producer.write_batch(), &cell, 1));
        let batch = consumer.try_reserve_read_batch(NonZeroUsize::MIN).unwrap();

        let writer_cell = cell.clone();
        let writer = thread::spawn(move || {
            let written = write(&mut producer.write_batch(), &writer_cell, 2);
            (producer, written)
        });
        // Advancing the private read cursor must not release this live batch.
        assert_eq!(read(&cell), 1);
        drop(batch);
        let (mut producer, written) = writer.join().unwrap();
        if !written {
            assert!(write(&mut producer.write_batch(), &cell, 2));
        }
        let batch = consumer.try_reserve_read_batch(NonZeroUsize::MIN).unwrap();
        assert_eq!(read(&cell), 2);
        drop(batch);
        assert!(consumer.try_read().is_none());
    });
}

#[test]
fn consuming_iterator_keeps_entire_reservation_until_drop() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue(1, 0);
        producer.try_write(1).unwrap();
        let batch = consumer.try_reserve_read_batch(NonZeroUsize::MIN).unwrap();
        let mut iter = batch.into_iter();
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);

        let writer = thread::spawn(move || {
            // Even exhausting the iterator must not release its reservation.
            assert_eq!(producer.try_write(2), Err(2));
            producer
        });
        let mut producer = writer.join().unwrap();
        drop(iter);
        producer.try_write(2).unwrap();
        assert_eq!(consumer.try_read(), Some(2));
    });
}

struct DropPayload(Option<(Payloads, usize)>);

impl Drop for DropPayload {
    fn drop(&mut self) {
        if let Some((cells, index)) = &self.0 {
            cells[*index].with_mut(|ptr| {
                // SAFETY: the consumer still owns this slot while dropping its
                // value. Loom checks that reuse cannot race the destructor.
                assert_eq!(unsafe { *ptr }, 1);
                thread::yield_now();
                // SAFETY: the same tracked exclusive access is still held.
                unsafe { *ptr = 2 };
            });
        }
    }
}

fn queue_with_drop_payloads() -> (Producer<DropPayload>, Consumer<DropPayload>, Payloads) {
    // The two-item reservation crosses the physical buffer boundary.
    let (mut producer, consumer) = queue(2, 1);
    let cells = Arc::new(vec![UnsafeCell::new(1), UnsafeCell::new(1)]);
    for index in [1, 0] {
        assert!(producer
            .try_write(DropPayload(Some((cells.clone(), index))))
            .is_ok());
    }
    (producer, consumer, cells)
}

fn try_reuse_after_drops(
    producer: &mut Producer<DropPayload>,
    cells: &[UnsafeCell<usize>],
) -> bool {
    producer.sync();
    // SAFETY: a successful reservation is initialized below before commit.
    let Some(slot) = (unsafe { producer.reserve() }) else {
        return false;
    };
    // The complete read reservation must remain pinned until every destructor
    // finishes. Check tracked state before writing to the actual raw slot.
    for cell in cells {
        assert_eq!(
            read(cell),
            2,
            "slot released before all destructors finished"
        );
    }
    // SAFETY: this slot was exclusively reserved, and DropPayload(None) is valid.
    unsafe { slot.write(DropPayload(None)) };
    producer.commit();
    true
}

#[test]
fn batch_drop_finishes_all_destructors_before_slot_reuse() {
    loom::model(|| {
        let (mut producer, mut consumer, cells) = queue_with_drop_payloads();
        let batch = consumer
            .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
            .unwrap();
        assert_eq!(batch.len(), 2);

        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            let reused = try_reuse_after_drops(&mut producer, &writer_cells);
            (producer, reused)
        });
        drop(batch);
        let (mut producer, reused) = writer.join().unwrap();
        if !reused {
            assert!(try_reuse_after_drops(&mut producer, &cells));
        }
        // Both slots become available together once all destructors finish.
        assert!(try_reuse_after_drops(&mut producer, &cells));
        assert!(!try_reuse_after_drops(&mut producer, &cells));
    });
}

#[test]
fn partial_iterator_drop_finishes_suffix_before_slot_reuse() {
    loom::model(|| {
        let (mut producer, mut consumer, cells) = queue_with_drop_payloads();
        let batch = consumer
            .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
            .unwrap();
        assert_eq!(batch.len(), 2);
        let mut iter = batch.into_iter();
        drop(iter.next().unwrap());
        assert_eq!(read(&cells[1]), 2);
        assert_eq!(read(&cells[0]), 1);

        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            let reused = try_reuse_after_drops(&mut producer, &writer_cells);
            (producer, reused)
        });
        // The first item is gone, but its slot stays reserved along with the
        // suffix until dropping the iterator finishes the remaining destructor.
        drop(iter);
        let (mut producer, reused) = writer.join().unwrap();
        if !reused {
            assert!(try_reuse_after_drops(&mut producer, &cells));
        }
        assert!(try_reuse_after_drops(&mut producer, &cells));
        assert!(!try_reuse_after_drops(&mut producer, &cells));
    });
}

#[test]
fn last_endpoint_reclaims_buffered_values() {
    loom::model(|| {
        let (producer, consumer, cells) = queue_with_drop_payloads();
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            // Even if the consumer is gone, this endpoint keeps the values live.
            for cell in writer_cells.iter() {
                assert_eq!(read(cell), 1);
            }
            drop(producer);
        });

        // Either endpoint may disappear first, but neither may reclaim values
        // while the other still owns the queue.
        for cell in cells.iter() {
            assert_eq!(read(cell), 1);
        }
        drop(consumer);
        writer.join().unwrap();
        for cell in cells.iter() {
            assert_eq!(read(cell), 2);
        }
        // Loom checks the queue and region Arc owners for leaks as the model ends.
    });
}

#[test]
fn reclamation_observes_final_publication_and_consumer_progress() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue::<DropPayload>(2, usize::MAX);
        let cells = Arc::new(vec![UnsafeCell::new(1), UnsafeCell::new(0)]);
        assert!(producer
            .try_write(DropPayload(Some((cells.clone(), 0))))
            .is_ok());

        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            writer_cells[1].with_mut(|ptr| {
                // SAFETY: this value has not been published yet, so its shadow
                // metadata is exclusively owned by the producer.
                unsafe { *ptr = 1 };
            });
            assert!(producer
                .try_write(DropPayload(Some((writer_cells, 1))))
                .is_ok());
            drop(producer);
        });

        // Consume only the first value. Cleanup must skip its released slot
        // and destroy the final publication exactly once, across cursor wrap.
        let batch = consumer.try_reserve_read_batch(NonZeroUsize::MIN).unwrap();
        drop(batch);
        assert_eq!(read(&cells[0]), 2);
        drop(consumer);
        writer.join().unwrap();
        for cell in cells.iter() {
            assert_eq!(read(cell), 2);
        }
        // Loom checks the queue and region Arc owners for leaks as the model ends.
    });
}

#[test]
fn publication_wakes_waiting_consumer() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue(1, usize::MAX);
        let cell = Arc::new(UnsafeCell::new(0));
        let reader_cell = cell.clone();
        let reader = thread::spawn(move || {
            let header = consumer.queue.header;
            // SAFETY: the consumer keeps its initialized header alive.
            let header = unsafe { header.as_ref() };
            // Match reserve_read_batch_timeout, omitting its 2048 spin attempts
            // so Loom explores registration, recheck, and compare-and-sleep.
            let reservation = header
                .waiters
                .wait_for(&header.write, 0, Duration::MAX, || {
                    consumer.queue.load_write();
                    consumer.try_reserve_read_range(NonZeroUsize::MIN)
                })
                .unwrap();
            let batch = consumer.read_batch_for(reservation);
            assert_eq!(batch.len(), 1);
            assert_eq!(read(&reader_cell), 42);
        });

        assert!(write(&mut producer.write_batch(), &cell, 42));
        reader.join().unwrap();
    });
}
