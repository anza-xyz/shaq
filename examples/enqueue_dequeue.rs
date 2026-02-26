#[path = "shared/common.rs"]
mod common;

use common::{
    cleanup_queue_file, prepare_queue_file, run_consumer_loop, run_producer_loop,
    run_total_throughput_loop, setup_exit_handler, Item, SYNC_CADENCE,
};
use shaq::{
    mpmc::{Consumer as MpmcConsumer, Producer as MpmcProducer},
    spsc::{Consumer as SpscConsumer, Producer as SpscProducer},
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

const QUEUE_SIZE: usize = 16 * 1024 * 1024;

enum Mode {
    Spsc,
    Mpmc { producers: usize, consumers: usize },
}

struct Config {
    mode: Mode,
    verbose: bool,
}

fn main() {
    let config = parse_config_or_exit();
    match config.mode {
        Mode::Spsc => run_spsc(config.verbose),
        Mode::Mpmc {
            producers,
            consumers,
        } => run_mpmc(producers, consumers, config.verbose),
    }
}

fn parse_config_or_exit() -> Config {
    let mut verbose = false;
    let mut positional = Vec::new();

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "-v" | "--verbose" => {
                verbose = true;
            }
            _ => positional.push(arg),
        }
    }

    let mode = match positional.first().map(String::as_str) {
        None | Some("spsc") => {
            if positional.len() > 1 {
                eprintln!("Too many arguments for spsc mode");
                print_usage();
                std::process::exit(2);
            }
            Mode::Spsc
        }
        Some("mpmc") => {
            let producers = parse_usize_arg(positional.get(1).cloned(), 2, "producers");
            let consumers = parse_usize_arg(positional.get(2).cloned(), 2, "consumers");
            if positional.len() > 3 {
                eprintln!("Too many arguments for mpmc mode");
                print_usage();
                std::process::exit(2);
            }
            Mode::Mpmc {
                producers,
                consumers,
            }
        }
        Some(mode) => {
            eprintln!("Unknown mode: {mode}");
            print_usage();
            std::process::exit(2);
        }
    };

    Config { mode, verbose }
}

fn parse_usize_arg(value: Option<String>, default: usize, name: &str) -> usize {
    value
        .map(|v| {
            v.parse::<usize>().unwrap_or_else(|_| {
                eprintln!("Invalid {name}: {v}");
                std::process::exit(2);
            })
        })
        .unwrap_or(default)
}

fn print_usage() {
    eprintln!(
        "Usage: cargo run --example enqueue_dequeue -- [-v|--verbose] [spsc|mpmc [producers] [consumers]]"
    );
}

fn run_spsc(verbose: bool) {
    let (consumer_core_id, producer_core_id) = spsc_core_ids();
    if let Some(core) = consumer_core_id {
        println!("Consumer core id: {}", core.id);
    }
    if let Some(core) = producer_core_id {
        println!("Producer core id: {}", core.id);
    }

    let exit = setup_exit_handler();
    let queue_path = "/tmp/shaq";
    let queue_file = prepare_queue_file(queue_path);
    let total_items_produced = Arc::new(AtomicU64::new(0));
    let producer_reserve_failures = Arc::new(AtomicU64::new(0));
    let consumer_reserve_failures = Arc::new(AtomicU64::new(0));

    let begin_signal = Arc::new(AtomicBool::new(false));
    let consumer_file = queue_file.try_clone().unwrap();
    let consumer_handle = std::thread::Builder::new()
        .name("shaqConsumer".to_string())
        .spawn({
            let begin = begin_signal.clone();
            let exit = exit.clone();
            let consumer_reserve_failures = consumer_reserve_failures.clone();
            move || {
                if let Some(core_id) = consumer_core_id {
                    core_affinity::set_for_current(core_id);
                }

                while !begin.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                // SAFETY: The file is created by the producer and uniquely joined as a consumer.
                let consumer = unsafe { SpscConsumer::join(&consumer_file) }.unwrap();
                run_spsc_consumer(consumer, exit, consumer_reserve_failures);
            }
        })
        .unwrap();

    let producer_handle = std::thread::Builder::new()
        .name("shaqProducer".to_string())
        .spawn({
            let exit = exit.clone();
            let total_items_produced = total_items_produced.clone();
            let producer_reserve_failures = producer_reserve_failures.clone();
            move || {
                if let Some(core_id) = producer_core_id {
                    core_affinity::set_for_current(core_id);
                }

                // SAFETY: This thread uniquely creates the queue.
                let producer = unsafe { SpscProducer::create(&queue_file, QUEUE_SIZE) }.unwrap();
                begin_signal.store(true, Ordering::Release);
                run_spsc_producer(
                    producer,
                    exit,
                    verbose,
                    total_items_produced,
                    producer_reserve_failures,
                );
            }
        })
        .unwrap();

    run_total_throughput_loop::<Item>(
        exit.clone(),
        total_items_produced,
        producer_reserve_failures,
        consumer_reserve_failures,
    );
    consumer_handle.join().unwrap();
    producer_handle.join().unwrap();
    cleanup_queue_file(queue_path);
}

fn run_spsc_producer(
    mut producer: SpscProducer<Item>,
    exit: Arc<AtomicBool>,
    verbose: bool,
    total_items_produced: Arc<AtomicU64>,
    producer_reserve_failures: Arc<AtomicU64>,
) {
    run_producer_loop::<Item, _>(
        exit,
        verbose.then(|| "Producer".to_string()),
        total_items_produced,
        move || {
            producer.sync();
            let mut produced = 0;
            for _ in 0..SYNC_CADENCE {
                // SAFETY: reserve() yields a valid write slot.
                let Some(mut spot) = (unsafe { producer.reserve() }) else {
                    producer_reserve_failures.fetch_add(1, Ordering::Relaxed);
                    break;
                };
                // SAFETY: the reserved slot can be fully initialized.
                unsafe {
                    spot.as_mut().data.fill(42);
                }
                produced += 1;
            }
            producer.commit();
            (produced > 0).then_some(produced)
        },
    );
}

fn run_spsc_consumer(
    mut consumer: SpscConsumer<Item>,
    exit: Arc<AtomicBool>,
    consumer_reserve_failures: Arc<AtomicU64>,
) {
    run_consumer_loop(exit, move || {
        consumer.sync();
        for _ in 0..SYNC_CADENCE {
            let Some(_item) = consumer.try_read() else {
                consumer_reserve_failures.fetch_add(1, Ordering::Relaxed);
                break;
            };
        }
        consumer.finalize();
    });
}

fn run_mpmc(producers: usize, consumers: usize, verbose: bool) {
    let (consumer_cores, producer_cores) = mpmc_core_ids(consumers, producers);
    let exit = setup_exit_handler();
    let queue_path = "/tmp/shaq_mpmc";
    let queue_file = prepare_queue_file(queue_path);
    let total_items_produced = Arc::new(AtomicU64::new(0));
    let producer_reserve_failures = Arc::new(AtomicU64::new(0));
    let consumer_reserve_failures = Arc::new(AtomicU64::new(0));

    // SAFETY: This thread uniquely creates the queue.
    unsafe {
        let _ = MpmcProducer::<Item>::create(&queue_file, QUEUE_SIZE).unwrap();
    }
    // SAFETY: Queue was created above; joining once and sharing handles is safe.
    let producer = Arc::new(unsafe { MpmcProducer::<Item>::join(&queue_file) }.unwrap());
    // SAFETY: Queue was created above; joining once and sharing handles is safe.
    let consumer = Arc::new(unsafe { MpmcConsumer::<Item>::join(&queue_file) }.unwrap());

    let mut handles = Vec::new();

    for (idx, core_id) in consumer_cores.into_iter().enumerate() {
        let exit = exit.clone();
        let consumer = consumer.clone();
        let consumer_reserve_failures = consumer_reserve_failures.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("shaqMpmcConsumer{idx}"))
                .spawn(move || {
                    if let Some(core_id) = core_id {
                        println!("Consumer {idx} core id: {}", core_id.id);
                        core_affinity::set_for_current(core_id);
                    }

                    run_mpmc_consumer(consumer, exit, consumer_reserve_failures);
                })
                .unwrap(),
        );
    }

    for (idx, core_id) in producer_cores.into_iter().enumerate() {
        let exit = exit.clone();
        let producer = producer.clone();
        let report_prefix = verbose.then(|| format!("Producer {idx}"));
        handles.push(
            std::thread::Builder::new()
                .name(format!("shaqMpmcProducer{idx}"))
                .spawn({
                    let total_items_produced = total_items_produced.clone();
                    let producer_reserve_failures = producer_reserve_failures.clone();
                    move || {
                        if let Some(core_id) = core_id {
                            println!("Producer {idx} core id: {}", core_id.id);
                            core_affinity::set_for_current(core_id);
                        }

                        run_mpmc_producer(
                            producer,
                            exit,
                            report_prefix,
                            total_items_produced,
                            producer_reserve_failures,
                        );
                    }
                })
                .unwrap(),
        );
    }

    run_total_throughput_loop::<Item>(
        exit.clone(),
        total_items_produced,
        producer_reserve_failures,
        consumer_reserve_failures,
    );
    for handle in handles {
        handle.join().unwrap();
    }

    cleanup_queue_file(queue_path);
}

fn run_mpmc_producer(
    producer: Arc<MpmcProducer<Item>>,
    exit: Arc<AtomicBool>,
    report_prefix: Option<String>,
    total_items_produced: Arc<AtomicU64>,
    producer_reserve_failures: Arc<AtomicU64>,
) {
    run_producer_loop::<Item, _>(exit, report_prefix, total_items_produced, move || {
        // SAFETY: we write the batch below.
        let Some(mut batch) = (unsafe { producer.reserve_write_batch(SYNC_CADENCE) }) else {
            producer_reserve_failures.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        for index in 0..batch.len() {
            // SAFETY: reserve_batch() yields valid contiguous slots.
            unsafe {
                batch.as_mut(index).data.fill(42);
            }
        }
        Some(batch.len())
    });
}

fn run_mpmc_consumer(
    consumer: Arc<MpmcConsumer<Item>>,
    exit: Arc<AtomicBool>,
    consumer_reserve_failures: Arc<AtomicU64>,
) {
    run_consumer_loop(exit, move || {
        let Some(batch) = consumer.reserve_read_batch(SYNC_CADENCE) else {
            consumer_reserve_failures.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let _ = batch.len();
    });
}

fn spsc_core_ids() -> (Option<core_affinity::CoreId>, Option<core_affinity::CoreId>) {
    core_affinity::get_core_ids()
        .map(|cores| {
            if cores.len() >= 2 {
                (Some(cores[cores.len() - 2]), Some(cores[cores.len() - 1]))
            } else {
                (None, cores.last().copied())
            }
        })
        .unwrap_or_default()
}

fn mpmc_core_ids(
    consumers: usize,
    producers: usize,
) -> (
    Vec<Option<core_affinity::CoreId>>,
    Vec<Option<core_affinity::CoreId>>,
) {
    core_affinity::get_core_ids()
        .map(|cores| {
            let mut index = cores.len();
            let mut consumer_cores = Vec::with_capacity(consumers);
            let mut producer_cores = Vec::with_capacity(producers);

            for _ in 0..consumers {
                if index == 0 {
                    consumer_cores.push(None);
                } else {
                    index -= 1;
                    consumer_cores.push(Some(cores[index]));
                }
            }

            for _ in 0..producers {
                if index == 0 {
                    producer_cores.push(None);
                } else {
                    index -= 1;
                    producer_cores.push(Some(cores[index]));
                }
            }

            (consumer_cores, producer_cores)
        })
        .unwrap_or_else(|| (vec![None; consumers], vec![None; producers]))
}
