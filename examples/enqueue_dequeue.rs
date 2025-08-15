use shaq::{Consumer, Producer};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

struct Item {
    data: [u8; 512],
}

fn main() {
    let (consumer_core_id, producer_core_id) = core_affinity::get_core_ids()
        .map(|c| {
            // Get the last 2 cores - assuming these are sorted.
            (Some(c[c.len() - 2]), Some(c[c.len() - 1]))
        })
        .unwrap_or_default();

    if let Some(consumer_core_id) = consumer_core_id.as_ref() {
        println!("Consumer core id: {}", consumer_core_id.id);
    }
    if let Some(producer_core_id) = producer_core_id.as_ref() {
        println!("Producer core id: {}", producer_core_id.id);
    }

    let exit = Arc::new(AtomicBool::new(false));
    ctrlc::set_handler({
        let exit = exit.clone();
        move || exit.store(true, Ordering::Release)
    })
    .unwrap();

    let queue_path = "/tmp/shaq";
    println!("Cleaning queue file: {queue_path}");
    let _ = std::fs::remove_file(queue_path);

    let begin_signal = Arc::new(AtomicBool::new(false));
    let consumer_hdl = std::thread::Builder::new()
        .name("shaqConsumer".to_string())
        .spawn({
            let begin = begin_signal.clone();
            let exit = exit.clone();
            move || {
                if let Some(id) = consumer_core_id {
                    core_affinity::set_for_current(id);
                }

                // Wait until the producer is ready - it creates the file.
                while !begin.load(Ordering::Acquire) {}
                let consumer = shaq::Consumer::join(queue_path).unwrap();
                run_consumer(consumer, exit)
            }
        })
        .unwrap();

    let producer_hdl = std::thread::Builder::new()
        .name("shaqProducer".to_string())
        .spawn(move || {
            if let Some(id) = producer_core_id {
                core_affinity::set_for_current(id);
            }

            let producer = shaq::Producer::create(queue_path, 16 * 1024 * 1024).unwrap();
            begin_signal.store(true, Ordering::Release);
            run_producer(producer, exit);
        })
        .unwrap();

    consumer_hdl.join().unwrap();
    producer_hdl.join().unwrap();

    println!("Cleaning up files");
    let _ = std::fs::remove_file(queue_path);
}

// Synchronize, Commit, Finalize every SYNC_CADENCE items.
const SYNC_CADENCE: usize = 1024;

fn run_producer(mut producer: Producer<Item>, exit: Arc<AtomicBool>) {
    let mut now = Instant::now();
    let mut items_produced = 0u64;

    while !exit.load(Ordering::Acquire) {
        producer.sync();
        for _ in 0..SYNC_CADENCE {
            let Some(mut spot) = producer.reserve() else {
                break;
            };
            unsafe {
                spot.as_mut().data.fill(42); // Fill with some data
            }
            items_produced += 1;
        }
        producer.commit();

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

fn run_consumer(mut consumer: Consumer<Item>, exit: Arc<AtomicBool>) {
    while !exit.load(Ordering::Acquire) {
        consumer.sync();
        for _ in 0..SYNC_CADENCE {
            let Some(_item) = consumer.try_read() else {
                break;
            };
        }
        consumer.finalize();
    }
}
