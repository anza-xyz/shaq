// use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
// use shaq::{create_buffer_mmap, join_buffer_mmap, Consumer, Producer, SharedQueue};

// const MAP_SIZE: usize = 10 * 1024 * 1024;

// fn bench_queue_with_size<const N: usize>(c: &mut Criterion) {
//     let header_path = format!("/tmp/shaq_bench_queue_header");
//     let buffer_path = format!("/mnt/hugepages/shaq_bench_queue_header_buffer");
//     let _ = std::fs::remove_file(&header_path);
//     let _ = std::fs::remove_file(&buffer_path);

//     let header_mmap = shaq::create_header_mmap(&header_path).unwrap();
//     let buffer_mmap = shaq::create_buffer_mmap(&buffer_path, MAP_SIZE).unwrap();
//     let mut queue = SharedQueue::new(header_mmap, buffer_mmap);

//     const NUM_ITEMS_PER_ITERATION: usize = 1024;
//     let mut group = c.benchmark_group(format!("queue/{N}"));
//     group.throughput(Throughput::Bytes((N * NUM_ITEMS_PER_ITERATION) as u64));
//     group.bench_function("push_pop", |b| {
//         b.iter(|| {
//             for _ in 0..NUM_ITEMS_PER_ITERATION {
//                 assert!(queue.try_enqueue(&[5; N]));
//                 let dequeued_item = queue.try_dequeue().unwrap();
//                 black_box(dequeued_item);
//             }
//         })
//     });
//     let _ = std::fs::remove_file(&header_path);
//     let _ = std::fs::remove_file(&buffer_path);
// }

// fn bench_producer_consumer_with_size<const N: usize>(c: &mut Criterion) {
//     let header_path = "/tmp/shaq_bench_producer_consumer_header";
//     let buffer_path = "/mnt/hugepages/shaq_bench_producer_consumer_buffer";
//     let _ = std::fs::remove_file(header_path);
//     let _ = std::fs::remove_file(buffer_path);
//     let mut producer = {
//         let header_mmap = shaq::create_header_mmap(&header_path).unwrap();
//         let buffer_mmap = create_buffer_mmap(buffer_path, MAP_SIZE).unwrap();
//         Producer::new(header_mmap, buffer_mmap)
//     };
//     let mut consumer = {
//         let header_mmap = shaq::join_header_mmap(&header_path).unwrap();
//         let buffer_mmap = join_buffer_mmap(buffer_path).unwrap();
//         Consumer::new(header_mmap, buffer_mmap)
//     };

//     const NUM_ITEMS_PER_ITERATION: usize = 1024;
//     let mut group = c.benchmark_group(format!("producer_consumer/{N}"));
//     group.throughput(Throughput::Bytes((N * NUM_ITEMS_PER_ITERATION) as u64));
//     group.bench_function("push_pop", |b| {
//         b.iter(|| {
//             producer.commit();
//             for _ in 0..NUM_ITEMS_PER_ITERATION {
//                 assert!(producer.try_enqueue(&[5; N]));
//             }
//             producer.commit();

//             consumer.sync();
//             for _ in 0..NUM_ITEMS_PER_ITERATION {
//                 let dequeued_item = consumer.try_dequeue().unwrap();
//                 black_box(dequeued_item);
//             }
//             consumer.sync();
//         })
//     });
//     let _ = std::fs::remove_file(buffer_path);
// }

// fn bench_queue(c: &mut Criterion) {
//     bench_queue_with_size::<138>(c);
//     bench_queue_with_size::<256>(c);
//     bench_queue_with_size::<512>(c);
//     bench_queue_with_size::<1024>(c);
//     bench_queue_with_size::<1232>(c);
// }

// fn bench_producer_consumer(c: &mut Criterion) {
//     bench_producer_consumer_with_size::<138>(c);
//     bench_producer_consumer_with_size::<256>(c);
//     bench_producer_consumer_with_size::<512>(c);
//     bench_producer_consumer_with_size::<1024>(c);
//     bench_producer_consumer_with_size::<1232>(c);
// }

// criterion_group!(benches, bench_queue, bench_producer_consumer);
// criterion_main!(benches);
