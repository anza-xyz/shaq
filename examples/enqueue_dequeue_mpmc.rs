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
        handles.push(
            std::thread::Builder::new()
                .name(format!("shaqMpmcConsumer{idx}"))
                .spawn(move || {
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
        handles.push(
            std::thread::Builder::new()
                .name(format!("shaqMpmcProducer{idx}"))
                .spawn(move || {
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

    let item = Item { data: [42; 512] };
    while !exit.load(Ordering::Acquire) {
        for _ in 0..SYNC_CADENCE {
            if producer.try_write(item).is_err() {
                break;
            }
            items_produced += 1;
        }

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
        for _ in 0..SYNC_CADENCE {
            if consumer.try_read().is_none() {
                break;
            }
        }
    }
}
