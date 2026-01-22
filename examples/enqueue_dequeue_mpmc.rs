use shaq::mpmc::{Consumer, Producer};
use std::{
    fs::File,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

#[derive(Clone, Copy)]
struct Item {
    data: [u8; 512],
}

fn main() {
    let mut args = std::env::args().skip(1);
    let producers: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(2);
    let consumers: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(2);

    let (consumer_cores, producer_cores) = core_affinity::get_core_ids()
        .map(|cores| {
            let mut index = cores.len();
            let mut consumer_cores = Vec::with_capacity(consumers);
            let mut producer_cores = Vec::with_capacity(producers);

            for _ in 0..consumers {
                if index == 0 {
                    consumer_cores.push(None);
                    continue;
                }
                index -= 1;
                consumer_cores.push(Some(cores[index]));
            }

            for _ in 0..producers {
                if index == 0 {
                    producer_cores.push(None);
                    continue;
                }
                index -= 1;
                producer_cores.push(Some(cores[index]));
            }

            (consumer_cores, producer_cores)
        })
        .unwrap_or_else(|| (vec![None; consumers], vec![None; producers]));

    let exit = Arc::new(AtomicBool::new(false));
    ctrlc::set_handler({
        let exit = exit.clone();
        move || exit.store(true, Ordering::Release)
    })
    .unwrap();

    let queue_path = "/tmp/shaq_mpmc";
    println!("Cleaning queue file: {queue_path}");
    let _ = std::fs::remove_file(queue_path);
    let queue_file = File::options()
        .create_new(true)
        .read(true)
        .write(true)
        .open(queue_path)
        .unwrap();

    let queue_size = 16 * 1024 * 1024;
    // SAFETY: The file is uniquely created here.
    unsafe {
        let _ = Producer::<Item>::create(&queue_file, queue_size).unwrap();
    }

    let mut handles = Vec::new();

    for idx in 0..consumers {
        let exit = exit.clone();
        let file = queue_file.try_clone().unwrap();
        let core_id = consumer_cores.get(idx).copied().flatten();
        handles.push(
            std::thread::Builder::new()
                .name(format!("shaqMpmcConsumer{idx}"))
                .spawn(move || {
                    if let Some(core_id) = core_id {
                        println!("Consumer {idx} core id: {}", core_id.id);
                        core_affinity::set_for_current(core_id);
                    }
                    // SAFETY: The file is created once and is uniquely joined as a Consumer.
                    let consumer = unsafe { Consumer::join(&file) }.unwrap();
                    run_consumer(consumer, exit);
                })
                .unwrap(),
        );
    }

    for idx in 0..producers {
        let exit = exit.clone();
        let file = queue_file.try_clone().unwrap();
        let report = idx == 0;
        let core_id = producer_cores.get(idx).copied().flatten();
        handles.push(
            std::thread::Builder::new()
                .name(format!("shaqMpmcProducer{idx}"))
                .spawn(move || {
                    if let Some(core_id) = core_id {
                        println!("Producer {idx} core id: {}", core_id.id);
                        core_affinity::set_for_current(core_id);
                    }
                    // SAFETY: The file is created once and is uniquely joined as a Producer.
                    let producer = unsafe { Producer::join(&file) }.unwrap();
                    run_producer(producer, exit, report);
                })
                .unwrap(),
        );
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("Cleaning up files");
    let _ = std::fs::remove_file(queue_path);
}

// Synchronize output every SYNC_CADENCE items.
const SYNC_CADENCE: usize = 1024;

fn run_producer(producer: Producer<Item>, exit: Arc<AtomicBool>, report: bool) {
    let mut now = Instant::now();
    let mut items_produced = 0u64;

    while !exit.load(Ordering::Acquire) {
        let Some(mut batch) = producer.reserve_batch(SYNC_CADENCE) else {
            continue;
        };
        for index in 0..batch.len() {
            // SAFETY: The batch reserves valid slots.
            unsafe {
                batch.as_mut(index).data.fill(42);
            }
        }
        items_produced += batch.len() as u64;

        if report {
            let new_now = Instant::now();
            if new_now.duration_since(now).as_secs() >= 1 {
                println!(
                    "{}/s ( GiB/s: {:.2} )",
                    items_produced,
                    items_produced as f64 * (core::mem::size_of::<Item>() as f64)
                        / (1024.0 * 1024.0 * 1024.0)
                );

                now = new_now;
                items_produced = 0;
            }
        }
    }
}

fn run_consumer(consumer: Consumer<Item>, exit: Arc<AtomicBool>) {
    while !exit.load(Ordering::Acquire) {
        if let Some(batch) = consumer.try_read_batch(SYNC_CADENCE) {
            let _ = batch.len();
        }
    }
}
