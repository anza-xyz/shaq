//! Bounded models of MPMC initialization, reservation, ordered publication and
//! release, reclamation, and waiter registration.
//!
//! Actual guards exercise the queue protocol. Loom-tracked shadow cells stand in
//! for raw payload accesses, which Loom cannot observe. Allocation/mapping, OS
//! futex behavior, process crashes, and finite timeouts are outside these models.
//! Spin loops use the shared hint abstraction: a processor hint in production,
//! and Loom's scheduling hint in models. These models assume scheduler progress.

use super::*;
use loom::{cell::UnsafeCell, sync::Arc, thread};

fn header(capacity: usize) -> SharedQueueHeader {
    SharedQueueHeader {
        magic: AtomicU64::new(MAGIC),
        version: VERSION,
        buffer_mask: u32::try_from(capacity - 1).unwrap(),
        identifier: DEFAULT_QUEUE_IDENTIFIER,
        producer_reservation: CacheAlignedAtomicSize::default(),
        producer_publication: CacheAlignedAtomicSize::default(),
        consumer_reservation: CacheAlignedAtomicSize::default(),
        consumer_release: CacheAlignedAtomicSize::default(),
        waiters: Waiters::default(),
    }
}

fn queue<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let size = minimum_region_size::<T>(capacity);
    let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
    let ptr = region.addr().cast::<SharedQueueHeader>();
    // Construct Loom atomics explicitly; zeroed memory does not register them.
    // SAFETY: the fresh region is sized and aligned for the header and buffer.
    unsafe { ptr.write(header(capacity)) };
    // SAFETY: the header was just initialized in this region.
    let producer = unsafe { Producer::from_header(region, ptr) }.unwrap();
    let consumer = producer.join_as_consumer();
    (producer, consumer)
}

type Payloads = Arc<Vec<UnsafeCell<usize>>>;

fn payloads(capacity: usize) -> Payloads {
    Arc::new((0..capacity).map(|_| UnsafeCell::new(0)).collect())
}

fn read(cell: &UnsafeCell<usize>) -> usize {
    cell.with(|ptr| {
        // SAFETY: the caller owns this logical payload. Loom checks ordering.
        unsafe { *ptr }
    })
}

fn write(cell: &UnsafeCell<usize>, value: usize) {
    cell.with_mut(|ptr| {
        // SAFETY: the caller owns this logical payload. Loom checks exclusivity.
        unsafe { *ptr = value };
    });
}

fn write_batch(producer: &Producer<usize>, cells: &[UnsafeCell<usize>], count: usize) -> bool {
    // SAFETY: every slot is initialized below before the batch is dropped.
    let Some(mut batch) =
        (unsafe { producer.try_reserve_write_batch(NonZeroUsize::new(count).unwrap()) })
    else {
        return false;
    };
    for offset in 0..batch.len() {
        let index = batch.start.wrapping_add(offset) & batch.buffer_mask;
        let value = read(&cells[index]) + 1;
        write(&cells[index], value);
        // SAFETY: offset is within the reservation.
        unsafe { batch.write(offset, value) };
    }
    true
}

fn read_batch(
    consumer: &Consumer<usize>,
    cells: &[UnsafeCell<usize>],
    max: usize,
) -> Option<Vec<usize>> {
    let batch = consumer.try_reserve_read_batch(NonZeroUsize::new(max).unwrap())?;
    Some(
        (0..batch.len())
            .map(|offset| {
                let index = batch.raw.start.wrapping_add(offset) & batch.raw.buffer_mask;
                let value = read(&cells[index]);
                assert_eq!(batch.get_owned(offset), Some(value));
                value
            })
            .collect(),
    )
}

#[test]
fn write_does_not_report_full_with_spare_capacity() {
    loom::model(|| {
        let (producer, consumer) = queue::<u64>(2);
        let other_producer = producer.clone();
        let other_consumer = consumer.clone();
        let worker = thread::spawn(move || {
            other_producer.try_write(1).unwrap();
            other_consumer.try_read().unwrap()
        });

        // Each producer writes once, so this queue can never be full
        // before this write, even if the worker completes an entire cycle.
        let writer = thread::spawn(move || producer.try_write(2).unwrap());
        writer.join().unwrap();
        let removed = worker.join().unwrap();
        let remaining = consumer.try_read().unwrap();
        let mut values = [removed, remaining];
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
        assert!(consumer.try_read().is_none());
    });
}

#[test]
fn read_does_not_report_empty_with_readable_items() {
    loom::model(|| {
        let (producer, consumer) = queue::<u64>(2);
        assert!(producer.try_write_slice(&[1, 2]));
        let other_consumer = consumer.clone();
        let worker = thread::spawn(move || {
            let removed = other_consumer.try_read().unwrap();
            producer.try_write(3).unwrap();
            removed
        });

        // The worker consumes only one of the two initial items, so an
        // item remains readable throughout this read attempt.
        let reader_consumer = consumer.clone();
        let reader = thread::spawn(move || reader_consumer.try_read().unwrap());
        let removed = reader.join().unwrap();
        let other_removed = worker.join().unwrap();
        let mut values = [removed, other_removed];
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
        assert_eq!(consumer.try_read(), Some(3));
        assert!(consumer.try_read().is_none());
    });
}

#[test]
fn initialization_publishes_header_metadata() {
    loom::model(|| {
        let header = Arc::new(header(2));
        header.magic.store(0, Ordering::Relaxed);
        let metadata = Arc::new(UnsafeCell::new((0, 0, 0)));
        let writer_header = header.clone();
        let writer_metadata = metadata.clone();
        let initializer = thread::spawn(move || {
            writer_metadata.with_mut(|ptr| {
                // SAFETY: the sole initializer owns metadata until publication.
                unsafe { *ptr = (VERSION, 1, DEFAULT_QUEUE_IDENTIFIER) };
            });
            writer_header.publish_initialization();
        });
        let joiner_header = header.clone();
        let joiner = thread::spawn(move || {
            if joiner_header.is_initialized() {
                metadata.with(|ptr| {
                    // SAFETY: observing magic acquires initialized header metadata.
                    let initialized = unsafe { *ptr };
                    assert_eq!(
                        initialized,
                        (
                            joiner_header.version,
                            joiner_header.buffer_mask,
                            joiner_header.identifier
                        )
                    );
                });
            }
        });
        joiner.join().unwrap();
        initializer.join().unwrap();
        assert!(header.is_initialized());
    });
}

#[test]
fn batch_publication_orders_payload() {
    loom::model(|| {
        let (producer, consumer) = queue(2);
        let cells = payloads(2);
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            assert!(write_batch(&producer, &writer_cells, 2));
        });
        let reader_cells = cells.clone();
        let reader = thread::spawn(move || {
            let values = read_batch(&consumer, &reader_cells, 2);
            (consumer, values)
        });

        let (consumer, values) = reader.join().unwrap();
        writer.join().unwrap();
        let values = values.or_else(|| read_batch(&consumer, &cells, 2)).unwrap();
        assert_eq!(values, [1, 1]);
        assert!(consumer.try_read().is_none());
    });
}

#[test]
fn competing_producers_reserve_disjoint_batches() {
    loom::model(|| {
        let (producer, consumer) = queue(4);
        let cells = payloads(4);
        let other = producer.clone();
        let other_cells = cells.clone();
        let writer = thread::spawn(move || {
            assert!(write_batch(&other, &other_cells, 2));
        });
        let second_cells = cells.clone();
        let second = thread::spawn(move || {
            assert!(write_batch(&producer, &second_cells, 2));
            producer
        });
        let producer = second.join().unwrap();
        writer.join().unwrap();
        assert!(!write_batch(&producer, &cells, 1));
        assert_eq!(read_batch(&consumer, &cells, 4).unwrap(), [1; 4]);
    });
}

#[test]
fn competing_consumers_reserve_disjoint_batches() {
    loom::model(|| {
        let (producer, consumer) = queue(4);
        let cells = payloads(4);
        assert!(write_batch(&producer, &cells, 4));
        let other = consumer.clone();
        let other_cells = cells.clone();
        let claim = |consumer: &Consumer<usize>, cells: &[UnsafeCell<usize>]| {
            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                .unwrap();
            assert_eq!(batch.len(), 2);
            for offset in 0..batch.len() {
                let index = batch.raw.start.wrapping_add(offset) & batch.raw.buffer_mask;
                assert_eq!(read(&cells[index]), 1);
                assert_eq!(batch.get_owned(offset), Some(1));
                write(&cells[index], 2);
            }
        };
        let reader = thread::spawn(move || claim(&other, &other_cells));
        let second_cells = cells.clone();
        let second = thread::spawn(move || {
            claim(&consumer, &second_cells);
            consumer
        });
        let consumer = second.join().unwrap();
        reader.join().unwrap();
        assert!(consumer.try_read().is_none());
        assert!(write_batch(&producer, &cells, 4));
        assert_eq!(read_batch(&consumer, &cells, 4).unwrap(), [3; 4]);
    });
}

#[test]
fn later_producer_publication_acquires_earlier_payload() {
    loom::model(|| {
        let (producer, consumer) = queue::<usize>(4);
        let cells = payloads(4);
        let other = producer.clone();
        let other_cells = cells.clone();
        let first_cells = cells.clone();
        let (reserved_tx, reserved_rx) = loom::sync::mpsc::channel();
        let first_writer = thread::spawn(move || {
            // Reserve before the second producer.
            // SAFETY: initialized below before publication.
            let first = unsafe { producer.try_reserve_write() }.unwrap();
            // Coordinate reservation order before touching the payload.
            reserved_tx.send(()).unwrap();
            write(&first_cells[0], 42);
            first.write(42);
        });
        let writer = thread::spawn(move || {
            reserved_rx.recv().unwrap();
            assert!(write_batch(&other, &other_cells, 2));
            // No join with the earlier producer can supply synchronization.
            assert_eq!(read_batch(&consumer, &other_cells, 4).unwrap(), [42, 1, 1]);
        });
        writer.join().unwrap();
        first_writer.join().unwrap();
    });
}

#[test]
fn later_consumer_release_acquires_earlier_reads() {
    loom::model(|| {
        let (producer, consumer) = queue(4);
        let cells = payloads(4);
        assert!(write_batch(&producer, &cells, 4));
        let first_consumer = consumer.clone();
        let first_cells = cells.clone();
        let other = consumer.clone();
        let other_cells = cells.clone();
        let (reserved_tx, reserved_rx) = loom::sync::mpsc::channel();
        let first_reader = thread::spawn(move || {
            let first = first_consumer.try_reserve_read().unwrap();
            // Coordinate reservation order before touching the payload.
            reserved_tx.send(()).unwrap();
            assert_eq!(read(&first_cells[0]), 1);
            write(&first_cells[0], 2);
            assert_eq!(first.into_inner(), 1);
        });
        let reader = thread::spawn(move || {
            reserved_rx.recv().unwrap();
            let batch = other
                .try_reserve_read_batch(NonZeroUsize::new(3).unwrap())
                .unwrap();
            assert_eq!(batch.len(), 3);
            for (index, cell) in other_cells[1..].iter().enumerate() {
                assert_eq!(read(cell), 1);
                assert_eq!(batch.get_owned(index), Some(1));
                write(cell, 2);
            }
            drop(batch);
            // Reuse without joining the earlier consumer: the actual
            // ordered release must carry both consumers' accesses.
            assert!(write_batch(&producer, &other_cells, 4));
        });
        reader.join().unwrap();
        first_reader.join().unwrap();
        assert_eq!(read_batch(&consumer, &cells, 4).unwrap(), [3; 4]);
    });
}

#[test]
fn slot_reuse_waits_for_consumer_release() {
    loom::model(|| {
        let (producer, consumer) = queue::<usize>(1);
        let cell = Arc::new(UnsafeCell::new(0));
        // SAFETY: initialized with write below.
        let guard = unsafe { producer.try_reserve_write() }.unwrap();
        write(&cell, 1);
        guard.write(42);
        let reader_cell = cell.clone();
        let reader = thread::spawn(move || {
            let guard = consumer.try_reserve_read().unwrap();
            assert_eq!(read(&reader_cell), 1);
            write(&reader_cell, 2);
            assert_eq!(guard.into_inner(), 42);
        });
        let writer_cell = cell.clone();
        let writer = thread::spawn(move || {
            // Reuse in an independent actor, before any join can acquire the
            // consumer's slot accesses on the producer's behalf.
            // SAFETY: any successful reservation is initialized below.
            let reused = unsafe { producer.try_reserve_write() }
                .map(|guard| {
                    assert_eq!(read(&writer_cell), 2);
                    write(&writer_cell, 3);
                    guard.write(43);
                })
                .is_some();
            (producer, reused)
        });
        let (producer, reused) = writer.join().unwrap();
        reader.join().unwrap();
        if !reused {
            // SAFETY: initialized immediately below.
            let guard = unsafe { producer.try_reserve_write() }.unwrap();
            assert_eq!(read(&cell), 2);
            guard.write(43);
        }
    });
}

struct DropPayload(Option<(Payloads, usize)>);

impl Drop for DropPayload {
    fn drop(&mut self) {
        if let Some((cells, index)) = &self.0 {
            cells[*index].with_mut(|ptr| {
                // SAFETY: this logical payload is exclusively owned until drop
                // finishes. Loom checks concurrent reclamation and slot reuse.
                assert_eq!(unsafe { *ptr }, 1);
                // SAFETY: the same tracked exclusive access is still held.
                unsafe { *ptr = 2 };
            });
        }
    }
}

fn queue_with_drop_payloads() -> (Producer<DropPayload>, Consumer<DropPayload>, Payloads) {
    let (producer, consumer) = queue(2);
    let cells = Arc::new(vec![UnsafeCell::new(1), UnsafeCell::new(1)]);
    for index in 0..2 {
        assert!(producer
            .try_write(DropPayload(Some((cells.clone(), index))))
            .is_ok());
    }
    (producer, consumer, cells)
}

fn try_reuse_after_drops(producer: &Producer<DropPayload>, cells: &[UnsafeCell<usize>]) -> bool {
    // SAFETY: initialized below before publishing.
    let Some(guard) = (unsafe { producer.try_reserve_write() }) else {
        return false;
    };
    for cell in cells {
        assert_eq!(
            read(cell),
            2,
            "batch released before all destructors finished"
        );
    }
    guard.write(DropPayload(None));
    true
}

#[test]
fn batch_drop_finishes_destructors_before_slot_reuse() {
    loom::model(|| {
        let (producer, consumer, cells) = queue_with_drop_payloads();
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            let reused = try_reuse_after_drops(&producer, &writer_cells);
            (producer, reused)
        });
        let reader = thread::spawn(move || {
            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                .unwrap();
            drop(batch);
            consumer
        });
        let _consumer = reader.join().unwrap();
        let (producer, reused) = writer.join().unwrap();
        if !reused {
            assert!(try_reuse_after_drops(&producer, &cells));
        }
        assert!(try_reuse_after_drops(&producer, &cells));
        assert!(!try_reuse_after_drops(&producer, &cells));
    });
}

#[test]
fn reclamation_observes_final_publication_and_consumer_progress() {
    loom::model(|| {
        let (producer, consumer) = queue::<DropPayload>(2);
        let cells = Arc::new(vec![UnsafeCell::new(0), UnsafeCell::new(1)]);
        assert!(producer
            .try_write(DropPayload(Some((cells.clone(), 1))))
            .is_ok());
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            write(&writer_cells[0], 1);
            assert!(producer
                .try_write(DropPayload(Some((writer_cells, 0))))
                .is_ok());
            drop(producer);
        });
        let reader_cells = cells.clone();
        let reader = thread::spawn(move || {
            // Cleanup must skip the consumed value and drop the final publication,
            // whichever endpoint's Arc release triggers reclamation.
            drop(consumer.try_reserve_read().unwrap());
            assert_eq!(read(&reader_cells[1]), 2);
            drop(consumer);
        });
        reader.join().unwrap();
        writer.join().unwrap();
        for cell in cells.iter() {
            assert_eq!(read(cell), 2);
        }
    });
}

#[test]
fn batch_publication_wakes_all_waiting_consumers() {
    // Bound this larger registration and reservation race for CI runtime.
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.check(|| {
        let (producer, consumer) = queue(2);
        let cells = payloads(2);
        let other = consumer.clone();
        let other_cells = cells.clone();
        let reader = thread::spawn(move || {
            let guard = other.reserve_read_timeout(Duration::MAX).unwrap();
            let index = guard.start & 1;
            assert_eq!(read(&other_cells[index]), 1);
            write(&other_cells[index], 2);
            assert_eq!(guard.into_inner(), 1);
            index
        });
        let reader_cells = cells.clone();
        let second = thread::spawn(move || {
            let batch = consumer
                .reserve_read_batch_timeout(NonZeroUsize::MIN, Duration::MAX)
                .unwrap();
            let index = batch.raw.start & 1;
            assert_eq!(read(&reader_cells[index]), 1);
            assert_eq!(batch.get_owned(0), Some(1));
            write(&reader_cells[index], 2);
            index
        });
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            assert!(write_batch(&producer, &writer_cells, 2));
            producer
        });
        assert_ne!(reader.join().unwrap(), second.join().unwrap());
        let producer = writer.join().unwrap();
        assert!(write_batch(&producer, &cells, 2));
    });
}
