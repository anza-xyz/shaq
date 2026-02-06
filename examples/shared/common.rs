use std::{
    fs::File,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
pub struct Item {
    pub data: [u8; 512],
}

// Synchronize/Batch size cadence.
pub const SYNC_CADENCE: usize = 1024;

pub fn setup_exit_handler() -> Arc<AtomicBool> {
    let exit = Arc::new(AtomicBool::new(false));
    ctrlc::set_handler({
        let exit = exit.clone();
        move || exit.store(true, Ordering::Release)
    })
    .unwrap();
    exit
}

pub fn prepare_queue_file(queue_path: &str) -> File {
    println!("Cleaning queue file: {queue_path}");
    let _ = std::fs::remove_file(queue_path);
    File::options()
        .create_new(true)
        .read(true)
        .write(true)
        .open(queue_path)
        .unwrap()
}

pub fn cleanup_queue_file(queue_path: &str) {
    println!("Cleaning up files");
    let _ = std::fs::remove_file(queue_path);
}

pub fn report_throughput<T>(now: &mut Instant, items_produced: &mut u64, prefix: &str) {
    let new_now = Instant::now();
    let elapsed = new_now.duration_since(*now).as_secs_f64();
    if elapsed >= 1.0 {
        let items_per_sec = *items_produced as f64 / elapsed;
        let gib_per_sec =
            items_per_sec * (core::mem::size_of::<T>() as f64) / (1024.0 * 1024.0 * 1024.0);
        println!("[{prefix}] {:.0} ({:.2} GiB/s)", items_per_sec, gib_per_sec);

        *now = new_now;
        *items_produced = 0;
    }
}

pub fn report_total_throughput<T>(
    now: &mut Instant,
    last_total_items: &mut u64,
    total_items_produced: &AtomicU64,
    prefix: &str,
) {
    let new_now = Instant::now();
    let elapsed = new_now.duration_since(*now).as_secs_f64();
    if elapsed >= 1.0 {
        let total_items = total_items_produced.load(Ordering::Relaxed);
        let items_delta = total_items.saturating_sub(*last_total_items);
        let items_per_sec = items_delta as f64 / elapsed;
        let gib_per_sec =
            items_per_sec * (core::mem::size_of::<T>() as f64) / (1024.0 * 1024.0 * 1024.0);
        println!("[{prefix}] {:.0} ({:.2} GiB/s)", items_per_sec, gib_per_sec);

        *now = new_now;
        *last_total_items = total_items;
    }
}

pub fn run_total_throughput_loop<T>(exit: Arc<AtomicBool>, total_items_produced: Arc<AtomicU64>) {
    let mut now = Instant::now();
    let mut last_total_items = 0u64;

    while !exit.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_secs(1));
        report_total_throughput::<T>(
            &mut now,
            &mut last_total_items,
            &total_items_produced,
            "Total",
        );
    }
}

pub fn run_producer_loop<T, F>(
    exit: Arc<AtomicBool>,
    report_prefix: Option<String>,
    total_items_produced: Arc<AtomicU64>,
    mut produce_batch: F,
) where
    F: FnMut() -> Option<usize>,
{
    let mut now = Instant::now();
    let mut items_produced = 0u64;

    while !exit.load(Ordering::Acquire) {
        let Some(produced) = produce_batch() else {
            continue;
        };
        total_items_produced.fetch_add(produced as u64, Ordering::Relaxed);
        items_produced += produced as u64;
        if let Some(prefix) = report_prefix.as_deref() {
            report_throughput::<T>(&mut now, &mut items_produced, prefix);
        }
    }
}

pub fn run_consumer_loop<F>(exit: Arc<AtomicBool>, mut consume_batch: F)
where
    F: FnMut(),
{
    while !exit.load(Ordering::Acquire) {
        consume_batch();
    }
}
