use super::*;

fn queue() -> (Producer<u64>, Consumer<u64>) {
    let size = minimum_region_size::<u64>(2);
    let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
    let header = region.addr().cast::<SharedQueueHeader>();
    // Construct Loom atomics explicitly: zeroed shared memory does not
    // register atomic objects with Loom's runtime.
    let value = SharedQueueHeader {
        magic: AtomicU64::new(MAGIC),
        version: VERSION,
        buffer_mask: 1,
        identifier: DEFAULT_QUEUE_IDENTIFIER,
        producer_reservation: CacheAlignedAtomicSize::default(),
        producer_publication: CacheAlignedAtomicSize::default(),
        consumer_reservation: CacheAlignedAtomicSize::default(),
        consumer_release: CacheAlignedAtomicSize::default(),
        waiters: Waiters::default(),
    };
    // SAFETY: the fresh region is sized and aligned for this header and
    // two u64 slots, and has no other users.
    unsafe { header.write(value) };
    // SAFETY: the header was just initialized in this region.
    let producer = unsafe { Producer::from_header(region, header) }.unwrap();
    let consumer = producer.join_as_consumer();
    (producer, consumer)
}

#[test]
fn write_does_not_report_full_with_spare_capacity() {
    loom::model(|| {
        let (producer, consumer) = queue();
        let other_producer = producer.clone();
        let worker = loom::thread::spawn(move || {
            // Model a competing producer/consumer cycle using only
            // cursors; no payload is accessed by this test.

            // SAFETY: the endpoint keeps the initialized header alive.
            let header = unsafe { other_producer.queue.header.as_ref() };
            if header
                .producer_reservation
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                header.producer_publication.store(1, Ordering::Release);
                header.consumer_reservation.store(1, Ordering::Release);
                header.consumer_release.store(1, Ordering::Release);
            }
        });

        // Each producer writes once, so this queue can never be full
        // before this write, even if the worker completes an entire cycle.
        assert!(producer
            .queue
            .reserve_write_batch(NON_ZERO_USIZE_ONE)
            .is_some());
        worker.join().unwrap();
        drop(consumer);
    });
}

#[test]
fn read_does_not_report_empty_with_readable_items() {
    loom::model(|| {
        let (producer, consumer) = queue();
        producer.try_write(1).unwrap();
        producer.try_write(2).unwrap();
        let other_consumer = consumer.clone();
        let worker = loom::thread::spawn(move || {
            // Model consuming one item and publishing its replacement.

            // SAFETY: the endpoint keeps the initialized header alive.
            let header = unsafe { other_consumer.queue.header.as_ref() };
            if header
                .consumer_reservation
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                header.consumer_release.store(1, Ordering::Release);
                header.producer_reservation.store(3, Ordering::Release);
                header.producer_publication.store(3, Ordering::Release);
            }
        });

        // The worker consumes only one of the two initial items, so an
        // item remains readable throughout this reservation attempt.
        assert!(consumer
            .queue
            .reserve_read_batch(NON_ZERO_USIZE_ONE)
            .is_some());
        worker.join().unwrap();
    });
}
