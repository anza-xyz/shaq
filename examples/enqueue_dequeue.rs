// use std::sync::{
//     atomic::{AtomicBool, Ordering},
//     Arc,
// };

// fn main() {
//     let item_size = std::env::args()
//         .skip(1)
//         .next()
//         .map(|s| s.parse::<usize>().expect("invalid item size"))
//         .unwrap_or(512);

//     let (recver_core_id, sender_core_id) = core_affinity::get_core_ids()
//         .map(|c| {
//             // Get the last 2 cores - assuming these are sorted.
//             (Some(c[c.len() - 2]), Some(c[c.len() - 1]))
//         })
//         .unwrap_or_default();

//     println!("Using item size: {} bytes", item_size);
//     if let Some(recver_core_id) = recver_core_id.as_ref() {
//         println!("Receiver core id: {}", recver_core_id.id);
//     }
//     if let Some(sender_core_id) = sender_core_id.as_ref() {
//         println!("Sender core id: {}", sender_core_id.id);
//     }

//     let exit = Arc::new(AtomicBool::new(false));
//     ctrlc::set_handler({
//         let exit = exit.clone();
//         move || exit.store(true, Ordering::Relaxed)
//     })
//     .unwrap();

//     let header_path = "/tmp/shaq_enqueue_dequeue_header";
//     let buffer_path = "/mnt/hugepages/shaq_enqueue_dequeue_buffer";
//     println!(
//         "Cleaning header file and buffer file: {} {}",
//         header_path, buffer_path
//     );
//     let _ = std::fs::remove_file(header_path);
//     let _ = std::fs::remove_file(buffer_path);

//     println!("Initializing files");
//     let header_ptr = shaq::create_header_mmap(header_path).unwrap();
//     let (buffer_ptr, file_size) = shaq::create_buffer_mmap(buffer_path, 16 * 1024 * 1024).unwrap();
//     let header_ptr = header_ptr as usize;
//     let buffer_ptr = buffer_ptr as usize;

//     let recver_hdl = std::thread::Builder::new()
//         .name("shaqRecver".to_string())
//         .spawn({
//             let exit = exit.clone();
//             move || {
//                 if let Some(id) = recver_core_id {
//                     core_affinity::set_for_current(id);
//                 }

//                 let recver = {
//                     let header_mmap = shaq::join_header_mmap(header_path).unwrap();
//                     let buffer_mmap = shaq::join_buffer_mmap(buffer_path).unwrap();
//                     shaq::Consumer::new(header_mmap, buffer_mmap)
//                 };
//                 println!("Receiver initialized");
//                 run_recver(recver, exit, item_size)
//             }
//         })
//         .unwrap();

//     let sender_hdl = std::thread::Builder::new()
//         .name("shaqSender".to_string())
//         .spawn(move || {
//             if let Some(id) = sender_core_id {
//                 core_affinity::set_for_current(id);
//             }

//             let header_ptr = header_ptr as *mut u8;
//             let buffer_ptr = buffer_ptr as *mut u8;
//             let sender = { shaq::Producer::new(header_ptr, (buffer_ptr, file_size)) };
//             println!("Sender initialized");
//             run_sender(sender, exit, item_size);
//         })
//         .unwrap();

//     recver_hdl.join().unwrap();
//     sender_hdl.join().unwrap();

//     println!("Cleaning up files");
//     let _ = std::fs::remove_file(header_path);
//     let _ = std::fs::remove_file(buffer_path);
// }

// const BYTES_PER_BATCH: usize = 100_000;

// #[inline(never)]
// fn run_sender(mut sender: shaq::Producer, exit: Arc<AtomicBool>, item_size: usize) {
//     let messages_per_batch = BYTES_PER_BATCH / item_size;

//     let mut message_count = 0;
//     let mut failed_reserves = 0;
//     let mut batch_count = 0;
//     let mut last_time = std::time::Instant::now();
//     let mut value = 0;
//     while !exit.load(Ordering::Relaxed) {
//         // Push in batches.
//         for _ in 0..messages_per_batch {
//             // Loop until we write the message.
//             loop {
//                 let Some(ptr) = sender.reserve(item_size) else {
//                     failed_reserves += 1;
//                     continue;
//                 };

//                 write_item(ptr, value, item_size);
//                 value += 1;
//                 message_count += 1;
//                 break;
//             }
//         }
//         sender.commit();

//         batch_count += 1;
//         if batch_count >= 100_000 {
//             let now = std::time::Instant::now();
//             let elapsed = now.duration_since(last_time);
//             last_time = now;
//             println!(
//                 "{:.02} GB/sec - {:.0} items/sec. ({} items, {} failed reserves)",
//                 (message_count * item_size) as f64 / (elapsed.as_secs_f64()) / 1e9,
//                 (message_count as f64) / elapsed.as_secs_f64(),
//                 message_count,
//                 failed_reserves
//             );
//             message_count = 0;
//             failed_reserves = 0;
//             batch_count = 0;
//         }
//     }
// }

// #[inline(never)]
// fn write_item(ptr: *mut u8, value: u8, item_size: usize) {
//     unsafe {
//         core::ptr::write_bytes(ptr, value, item_size);
//     }
// }

// #[inline(never)]
// fn run_recver(mut recver: shaq::Consumer, exit: Arc<AtomicBool>, item_size: usize) {
//     let messages_per_batch = BYTES_PER_BATCH / item_size;

//     let mut value = 0u8;
//     while !exit.load(Ordering::Relaxed) {
//         recver.sync();
//         let mut num_messages = 0;
//         loop {
//             let Some(buffer) = recver.try_dequeue() else {
//                 break;
//             };

//             let b = buffer[0];
//             if b != value {
//                 exit.store(true, Ordering::Relaxed);
//                 println!("Error: expected {}, got {} ({})", value, buffer[0], b);
//                 return;
//             }
//             value += 1;

//             num_messages += 1;
//             if num_messages == messages_per_batch {
//                 recver.sync();
//             }
//         }
//     }
// }
