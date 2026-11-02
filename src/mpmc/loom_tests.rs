use super::*;
use loom::thread;

fn header(capacity: usize, position: usize) -> SharedQueueHeader {
    let value = SharedQueueHeader {
        magic: AtomicU64::new(MAGIC),
        version: VERSION,
        buffer_mask: u32::try_from(capacity - 1).unwrap(),
        identifier: DEFAULT_QUEUE_IDENTIFIER,
        producer_reservation: CacheAlignedAtomicSize::default(),
        producer_publication: CacheAlignedAtomicSize::default(),
        consumer_reservation: CacheAlignedAtomicSize::default(),
        consumer_release: CacheAlignedAtomicSize::default(),
        waiters: Waiters::default(),
    };
    value
        .producer_reservation
        .store(position, Ordering::Relaxed);
    value
        .producer_publication
        .store(position, Ordering::Relaxed);
    value
        .consumer_reservation
        .store(position, Ordering::Relaxed);
    value.consumer_release.store(position, Ordering::Relaxed);
    value
}

fn queue<T>(capacity: usize, position: usize) -> (Producer<T>, Consumer<T>) {
    let size = minimum_region_size::<T>(capacity);
    let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
    let ptr = region.addr().cast::<SharedQueueHeader>();
    // Construct Loom atomics explicitly; zeroed memory does not register them.
    // SAFETY: the fresh region is sized and aligned for the header and buffer.
    unsafe { ptr.write(header(capacity, position)) };
    // SAFETY: the header was just initialized in this region.
    let producer = unsafe { Producer::from_header(region, ptr) }.unwrap();
    let consumer = producer.join_as_consumer();
    (producer, consumer)
}

#[test]
fn write_does_not_report_full_with_spare_capacity() {
    loom::model(|| {
        let (producer, consumer) = queue::<u64>(2, 0);
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
        let (producer, consumer) = queue::<u64>(2, 0);
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
