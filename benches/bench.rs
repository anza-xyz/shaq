use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use shaq::{Consumer, Producer};

const MAP_SIZE: usize = 10 * 1024 * 1024;

struct BenchItem<const N: usize> {
    data: [u8; N],
}

fn bench_queue_with_size<const N: usize>(c: &mut Criterion) {
    let queue_path = "/tmp/shaq_bench_queue";

    let _ = std::fs::remove_file(queue_path);
    let size = MAP_SIZE;

    let mut producer = Producer::<BenchItem<N>>::create(queue_path, size).unwrap();
    let mut consumer = Consumer::<BenchItem<N>>::join(queue_path).unwrap();

    const NUM_ITEMS_PER_ITERATION: usize = 1024;
    let mut group = c.benchmark_group(format!("queue/{N}"));
    group.throughput(Throughput::Bytes((N * NUM_ITEMS_PER_ITERATION) as u64));
    group.bench_function("push_pop", |b| {
        b.iter(|| {
            // Sync. Enqueue. Commit.
            producer.sync();
            for _ in 0..NUM_ITEMS_PER_ITERATION {
                let mut spot = producer.reserve().unwrap();
                unsafe { spot.as_mut() }.data = [5; N];
            }
            producer.commit();

            // Sync. Dequeue. Finalize.
            consumer.sync();
            for _ in 0..NUM_ITEMS_PER_ITERATION {
                let dequeued_item = consumer.try_read().unwrap();
                black_box(unsafe { dequeued_item.as_ref() });
            }
            consumer.finalize();
        })
    });
    let _ = std::fs::remove_file(queue_path);
}

fn bench_queue(c: &mut Criterion) {
    bench_queue_with_size::<138>(c);
    bench_queue_with_size::<256>(c);
    bench_queue_with_size::<512>(c);
    bench_queue_with_size::<1024>(c);
    bench_queue_with_size::<1232>(c);
}

criterion_group!(benches, bench_queue);
criterion_main!(benches);
