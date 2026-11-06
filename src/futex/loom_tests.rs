//! Models of the multi-source wait protocol in [`Waiters::wait_for_any`].
//!
//! Each source is a bare waiter count plus a futex word, woken the way the
//! real queues wake: either the word is the data cursor, advanced (Release)
//! before [`Waiters::wake`] (spsc/mpmc), or the data is published separately
//! and [`Waiters::bump_and_wake`] bumps the word (broadcast). The futex model
//! in `imp` lets one thread sleep on several words at once, like Linux
//! `futex_waitv`. Timeouts are not modeled.

use super::*;
use loom::{sync::Arc, thread};

#[derive(Clone, Copy, Debug)]
enum Kind {
    /// The word is the publication cursor (spsc/mpmc).
    Cursor,
    /// The word is a wake counter bumped only for registered waiters
    /// (broadcast).
    Bump,
}

struct Source {
    kind: Kind,
    waiters: Waiters,
    word: AtomicUsize,
    /// Published data for [`Kind::Bump`]; unused for [`Kind::Cursor`].
    data: AtomicUsize,
}

impl Source {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            waiters: Waiters::default(),
            word: AtomicUsize::new(0),
            data: AtomicUsize::new(0),
        }
    }

    fn publish(&self) {
        match self.kind {
            Kind::Cursor => {
                self.word.fetch_add(1, Ordering::Release);
                self.waiters.wake(&self.word, 1);
            }
            Kind::Bump => {
                self.data.store(1, Ordering::Release);
                self.waiters.bump_and_wake(&self.word);
            }
        }
    }

    fn wait_source(&self) -> WaitSource<'_> {
        WaitSource::new(&self.waiters, &self.word)
    }

    fn published(&self) -> bool {
        let data = match self.kind {
            Kind::Cursor => &self.word,
            Kind::Bump => &self.data,
        };
        data.load(Ordering::Acquire) != 0
    }
}

const KINDS: [(Kind, Kind); 3] = [
    (Kind::Cursor, Kind::Cursor),
    (Kind::Bump, Kind::Bump),
    (Kind::Cursor, Kind::Bump),
];

fn wait_for_either(a: &Source, b: &Source) -> usize {
    let sources = [a.wait_source(), b.wait_source()];
    Waiters::wait_for_any(&sources, 0, Duration::MAX, || {
        [a, b].iter().position(|source| source.published())
    })
    .unwrap()
}

#[test]
fn publication_on_either_source_wakes_waiter() {
    for (kind_a, kind_b) in KINDS {
        for target in 0..2 {
            loom::model(move || {
                let a = Arc::new(Source::new(kind_a));
                let b = Arc::new(Source::new(kind_b));
                let waiter = {
                    let (a, b) = (a.clone(), b.clone());
                    thread::spawn(move || wait_for_either(&a, &b))
                };

                [&a, &b][target].publish();
                assert_eq!(waiter.join().unwrap(), target);
            });
        }
    }
}

#[test]
fn publications_on_both_sources_leave_no_registration() {
    for (kind_a, kind_b) in KINDS {
        loom::model(move || {
            let a = Arc::new(Source::new(kind_a));
            let b = Arc::new(Source::new(kind_b));
            let waiter = {
                let (a, b) = (a.clone(), b.clone());
                thread::spawn(move || wait_for_either(&a, &b))
            };

            a.publish();
            b.publish();
            let selected = waiter.join().unwrap();
            assert!([&a, &b][selected].published());
            assert_eq!(a.waiters.waiters.load(Ordering::Relaxed), 0);
            assert_eq!(b.waiters.waiters.load(Ordering::Relaxed), 0);
            // The second wake may land on the sleeper's stale entry for the
            // other word; it must still have unqueued itself from every word.
            assert_eq!(imp::queued_sleepers(), 0);
        });
    }
}
