//! Futex-backed waiting for the shared-memory queues.
//!
//! The futex word is a 64-bit atomic whose low 32 bits change on (or just
//! before) every wake. For the single-cursor queues it is the publication
//! cursor itself — every publish advances it. A multi-cursor queue (the
//! broadcast, which has one cursor per lane) instead points waiters at a
//! dedicated wake counter that a publish bumps only when a waiter is present
//! (see [`Waiters::bump_and_wake`]); there the caller's `check` is what gates
//! on real data, and the counter exists only to break a racing `FUTEX_WAIT`.
//!
//! # Why no wake is lost
//!
//! The producer advances the futex word (Release), then issues a SeqCst fence
//! and loads `waiters`, skipping the wake syscall if it is zero. A waiter
//! increments `waiters`, issues a SeqCst fence (in `register`), then rechecks
//! for data before sleeping. SeqCst fences are totally ordered, and a load
//! after the later fence must see a store made before the earlier one. So
//! either the producer's load sees the waiter and wakes it, or the waiter's
//! recheck sees the new data (or `FUTEX_WAIT` bounces with `EAGAIN`) and it
//! never sleeps. Without the fences both loads can be stale and a wake can be
//! lost.
//!
//! A thread waiting on several queues at once ([`Waiters::wait_for_any`])
//! increments every queue's `waiters` before its single fence, so the argument
//! holds for each queue independently.
//!
//! # The futex word
//!
//! Userspace accesses the word only as a 64-bit atomic; the kernel's 32-bit
//! `FUTEX_WAIT` compare reads its low half (byte offset 0 on little-endian, 4
//! on big-endian), which is 4-byte aligned and cannot tear. Never materialize
//! an `&AtomicU32` into it in Rust code — only a raw pointer passed to the
//! syscall. If the word advances by an exact multiple of 2^32 between snapshot
//! and compare the wait oversleeps; that is bounded by the timeout and
//! astronomically unlikely.

use crate::sync::atomic::{fence, AtomicUsize, Ordering};
use crate::{error::WaitError, CacheAlignedAtomicSize};
use core::hint::spin_loop;
use std::time::{Duration, Instant};

/// Snapshot of a queue's 64-bit publication cursor.
type SequenceNumber = usize;

/// `None` when the timeout overflows `Instant`: the wait is unbounded.
fn deadline_from_timeout(timeout: Duration) -> Option<Instant> {
    Instant::now().checked_add(timeout)
}

#[cfg(not(loom))]
fn remaining_until(deadline: Instant) -> Result<Duration, WaitError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(WaitError::Timeout)
}

#[derive(Default)]
#[repr(C)]
pub(crate) struct Waiters {
    /// Approximate count of waiters registered against the queue's
    /// publication cursor.
    waiters: CacheAlignedAtomicSize,
}

impl Waiters {
    /// Initializes the waiter count inside a newly created shared-memory header.
    pub(crate) fn initialize(&self) {
        self.waiters.store(0, Ordering::Release);
    }

    /// Runs `check` until it returns a value or `timeout` elapses.
    ///
    /// Each source's cursor is used as the futex word while sleeping; an
    /// advance of any source's cursor must imply that `check` can observe new
    /// data.
    ///
    /// `spins` is how many times to re-`check` before the first sleep. A caller
    /// whose `check` is itself expensive (e.g. it scans many lanes) should pass
    /// a smaller count so the total spin work stays bounded; see
    /// [`SPIN_ATTEMPTS`] for the baseline used by a unit-cost check.
    ///
    /// With more than one source, a returning caller may have absorbed a wake
    /// meant for a source it did not take data from. A woken multi-source
    /// sleeper can also still count toward wakes issued on its other words
    /// until it runs (as with Linux `futex_waitv`). Callers whose sources can
    /// have competing waiters must therefore pass a wake on to those sources
    /// after sleeping, or another waiter can sleep next to available data.
    pub(crate) fn wait_for_any<const N: usize, T>(
        sources: &[WaitSource<'_>; N],
        spins: usize,
        timeout: Duration,
        mut check: impl FnMut() -> Option<T>,
    ) -> Result<T, WaitError> {
        const {
            assert!(
                N >= 1 && N <= MAX_WAIT_SOURCES,
                "wait sources must be between 1 and MAX_WAIT_SOURCES"
            )
        };

        if let Some(value) = check() {
            return Ok(value);
        }

        // Taken only after the first check so an immediately satisfied call
        // never reads the clock; the timeout still bounds the spin below.
        let deadline = deadline_from_timeout(timeout);

        // Spin only before the first sleep; once this thread has blocked it
        // is on the slow path and goes straight back to waiting.
        for _ in 0..spins {
            spin_loop();
            if let Some(value) = check() {
                return Ok(value);
            }
        }

        let mut snapshots = [0; N];
        loop {
            Self::register(sources, &mut snapshots);
            // Recheck after registering because a producer can publish after
            // the unregistered check and before this thread starts waiting;
            // the fence in `register` makes this recheck reliable (see
            // module docs).
            if let Some(value) = check() {
                Self::unregister(sources);
                return Ok(value);
            }

            // Platform waits can return after a matching wake or a spurious
            // wake, so success only means the caller's condition should be
            // checked again.
            let wait_result = Self::wait(sources, &snapshots, deadline);
            Self::unregister(sources);
            wait_result?;

            if let Some(value) = check() {
                return Ok(value);
            }
        }
    }

    /// [`Self::wait_for_any`] on this queue alone, with `cursor` (the queue's
    /// publication cursor or wake counter) as the futex word.
    pub(crate) fn wait_for<T>(
        &self,
        cursor: &AtomicUsize,
        spins: usize,
        timeout: Duration,
        check: impl FnMut() -> Option<T>,
    ) -> Result<T, WaitError> {
        Self::wait_for_any(&[WaitSource::new(self, cursor)], spins, timeout, check)
    }

    /// Registers this thread as a waiter and snapshots the cursor.
    /// Done for every source.
    ///
    /// Increment-then-fence is the waiter half of the lost-wake protocol
    /// (see module docs); the caller must recheck for data after this call,
    /// before sleeping.
    ///
    /// One fence after all increments suffices: each increment is sequenced
    /// before it, so a producer whose fence follows this one observes the
    /// increment on its own source.
    fn register<const N: usize>(
        sources: &[WaitSource<'_>; N],
        snapshots: &mut [SequenceNumber; N],
    ) {
        for source in sources {
            source.waiters.waiters.fetch_add(1, Ordering::Relaxed);
        }
        fence(Ordering::SeqCst);
        for (source, snapshot) in sources.iter().zip(snapshots.iter_mut()) {
            *snapshot = source.word.load(Ordering::Acquire);
        }
    }

    fn unregister<const N: usize>(sources: &[WaitSource<'_>; N]) {
        for source in sources {
            source.waiters.waiters.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn wait<const N: usize>(
        sources: &[WaitSource<'_>; N],
        expected: &[SequenceNumber; N],
        deadline: Option<Instant>,
    ) -> Result<(), WaitError> {
        // Avoid entering the platform wait backend if a publication already
        // advanced the cursor after the caller's post-registration recheck.
        if any_changed(sources, expected) {
            return Ok(());
        }

        // A single source keeps using plain `FUTEX_WAIT`
        if N == 1 {
            imp::wait(sources[0].word, expected[0], deadline)
        } else {
            imp::wait_any(sources, expected, deadline)
        }
    }

    /// Wakes up to `count` waiters registered against `cursor`.
    ///
    /// Callers must publish `cursor` with (at least) a Release store before
    /// calling this; the fence here pairs that store with a registering
    /// waiter (see module docs). The publication itself must be
    /// unconditional — only the wake syscall is elided when no waiter is
    /// registered.
    pub(crate) fn wake(&self, cursor: &AtomicUsize, count: usize) {
        debug_assert!(count > 0);

        // Fence-then-load is the producer half of the lost-wake protocol.
        // A plain load keeps the hot publish path free of RMWs on a line
        // consumers rarely write.
        fence(Ordering::SeqCst);
        let waiters = self.waiters.load(Ordering::Relaxed);
        if waiters == 0 {
            return;
        }

        let count = waiters.min(count).min(MAX_WAKE_COUNT) as u32;
        imp::wake(cursor, count);
    }

    /// Bumps `word` and wakes all registered waiters — but only if any are
    /// registered, so a queue with no blocked waiter never writes the shared
    /// `word` on its hot path.
    ///
    /// Unlike [`Self::wake`], the bump is conditional and done here: `word` is a
    /// dedicated wake counter that the caller does **not** otherwise advance, so
    /// this increment is what breaks a racing waiter's `FUTEX_WAIT`. Callers must
    /// have published the real data (the lane cursors a waiter rechecks) with a
    /// Release store before calling; the fence here pairs that with a registering
    /// waiter (see module docs).
    pub(crate) fn bump_and_wake(&self, word: &AtomicUsize) {
        fence(Ordering::SeqCst);
        let waiters = self.waiters.load(Ordering::Relaxed);
        if waiters == 0 {
            return;
        }

        // Change the futex word so a waiter that snapshotted it but has not yet
        // slept bounces out of `FUTEX_WAIT` with `EAGAIN`.
        word.fetch_add(1, Ordering::Relaxed);
        let count = waiters.min(MAX_WAKE_COUNT) as u32;
        imp::wake(word, count);
    }
}

const MAX_WAKE_COUNT: usize = i32::MAX as usize;

/// Maximum number of queues one wait can sleep on at once.
///
/// Checked at compile time by [`Waiters::wait_for_any`], equal to kernel's
/// `FUTEX_WAITV_MAX` (128).
pub(crate) const MAX_WAIT_SOURCES: usize = 128;

/// One queue a waiter can sleep on: the queue's waiter count and the futex
/// word its producers advance (or bump) before waking.
#[derive(Clone, Copy)]
pub(crate) struct WaitSource<'a> {
    waiters: &'a Waiters,
    word: &'a AtomicUsize,
}

impl<'a> WaitSource<'a> {
    pub(crate) fn new(waiters: &'a Waiters, word: &'a AtomicUsize) -> Self {
        Self { waiters, word }
    }
}

/// Returns whether any source's word moved away from its snapshot.
fn any_changed<const N: usize>(
    sources: &[WaitSource<'_>; N],
    expected: &[SequenceNumber; N],
) -> bool {
    sources
        .iter()
        .zip(expected)
        .any(|(source, &expected)| source.word.load(Ordering::Acquire) != expected)
}

/// Polls until any source's word no longer equals its snapshot or the
/// deadline passes, yielding the thread between checks.
///
/// While a thread polls here it stays registered on every source, so each
/// publish on those sources still issues a wake syscall that finds no sleeper.
#[cfg(not(loom))]
fn poll_any<const N: usize>(
    sources: &[WaitSource<'_>; N],
    expected: &[SequenceNumber; N],
    deadline: Option<Instant>,
) -> Result<(), WaitError> {
    loop {
        if any_changed(sources, expected) {
            return Ok(());
        }

        if let Some(deadline) = deadline {
            remaining_until(deadline)?;
        }

        std::thread::yield_now();
    }
}

/// Baseline `check` attempts before a waiter's first sleep in
/// [`Waiters::wait_for`], for a unit-cost `check`. Callers with a costlier
/// `check` scale this down so the total spin work stays comparable.
#[cfg(not(loom))]
pub(crate) const SPIN_ATTEMPTS: usize = 2048;

// Explore registration and sleep races directly, without modeling busy spins.
#[cfg(loom)]
pub(crate) const SPIN_ATTEMPTS: usize = 0;

#[cfg(all(target_os = "linux", not(loom)))]
mod imp {
    use super::{poll_any, remaining_until, SequenceNumber, WaitSource};
    use crate::error::WaitError;
    use crate::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    /// Polls until any source's word changes; see [`super::poll_any`].
    ///
    /// Only waits on more than one source get here; single-source waits use
    /// [`wait`] (`FUTEX_WAIT`).
    pub(super) fn wait_any<const N: usize>(
        sources: &[WaitSource<'_>; N],
        expected: &[SequenceNumber; N],
        deadline: Option<Instant>,
    ) -> Result<(), WaitError> {
        poll_any(sources, expected, deadline)
    }

    /// Returns the futex word: the low 32 bits of the 64-bit cursor, the
    /// half that changes on every publication.
    ///
    /// Only the kernel reads through this pointer; never materialize an
    /// `&AtomicU32` over the cursor in Rust code (see module docs).
    fn futex_word(cursor: &AtomicUsize) -> *mut u32 {
        let ptr = cursor.as_ptr().cast();
        // The low half lives at byte offset 4 on big-endian targets.
        #[cfg(target_endian = "big")]
        // SAFETY: in bounds; the low half of the 8-byte cursor is at offset 4.
        let ptr = unsafe { ptr.add(1) };
        ptr
    }

    /// Blocks with Linux `FUTEX_WAIT` while the low 32 bits of `cursor` still
    /// equal the low 32 bits of `expected`.
    ///
    /// `Ok(())` means the caller should recheck its own condition; Linux can
    /// return success for ordinary wakes and for spurious wakes.
    pub(super) fn wait(
        cursor: &AtomicUsize,
        expected: SequenceNumber,
        deadline: Option<Instant>,
    ) -> Result<(), WaitError> {
        let expected = expected as u32;
        loop {
            // A null timeout makes `FUTEX_WAIT` block indefinitely.
            let timeout_storage = match deadline {
                Some(deadline) => Some(duration_to_timespec(remaining_until(deadline)?)),
                None => None,
            };
            let timeout_ptr = timeout_storage
                .as_ref()
                .map_or(core::ptr::null(), |timeout| {
                    timeout as *const libc::timespec
                });

            // SAFETY:
            // - `futex_word(cursor)` is a valid, 4-byte aligned pointer into a
            //   live 64-bit atomic; the kernel only reads 32 bits through it.
            // - `timeout_ptr` is null (block indefinitely) or points to a
            //   live `timespec`.
            // - `FUTEX_WAIT` only blocks if the value still equals `expected`.
            // - No `FUTEX_PRIVATE_FLAG`: the cursor lives in cross-process
            //   shared memory.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    futex_word(cursor),
                    libc::FUTEX_WAIT,
                    expected as libc::c_int,
                    timeout_ptr,
                )
            };

            if result == 0 {
                return Ok(());
            }

            if result != -1 {
                panic!("unexpected futex wait result: {result}");
            }

            match errno() {
                libc::EAGAIN => return Ok(()),
                libc::EINTR => continue,
                libc::ETIMEDOUT => return Err(WaitError::Timeout),
                err => panic!("unexpected futex wait error: errno={err}"),
            }
        }
    }

    /// Wakes waiters blocked in Linux `FUTEX_WAIT` on `cursor`.
    pub(super) fn wake(cursor: &AtomicUsize, count: u32) {
        debug_assert!(count <= libc::c_int::MAX as u32);

        // SAFETY:
        // - `futex_word(cursor)` is a valid, 4-byte aligned pointer into a
        //   live 64-bit atomic.
        // - No `FUTEX_PRIVATE_FLAG`: the cursor lives in cross-process
        //   shared memory.
        let result = unsafe {
            libc::syscall(
                libc::SYS_futex,
                futex_word(cursor),
                libc::FUTEX_WAKE,
                count as libc::c_int,
            )
        };
        debug_assert!(
            result >= 0,
            "unexpected futex wake error: errno={}",
            errno()
        );
    }

    /// Converts a [`Duration`] to the relative [`libc::timespec`]
    /// timeout format expected by futex.
    #[inline]
    const fn duration_to_timespec(duration: Duration) -> libc::timespec {
        // Clamp instead of casting: a wrapped-negative `tv_sec` would make
        // the kernel reject the timespec with EINVAL.
        let secs = duration.as_secs();
        let tv_sec = if secs > libc::time_t::MAX as u64 {
            libc::time_t::MAX
        } else {
            secs as libc::time_t
        };
        libc::timespec {
            tv_sec,
            tv_nsec: duration.subsec_nanos() as libc::c_long,
        }
    }

    fn errno() -> i32 {
        // SAFETY: `__errno_location` returns this thread's errno location on Linux.
        let errno = unsafe { libc::__errno_location() };
        // SAFETY: the returned pointer is valid and points to this thread's errno.
        unsafe { *errno }
    }
}

#[cfg(all(not(target_os = "linux"), not(loom)))]
mod imp {
    use super::{poll_any, remaining_until, SequenceNumber, WaitSource};
    use crate::error::WaitError;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    /// Polls until `cursor` no longer equals `expected` or timeout elapses,
    /// yielding the thread between checks.
    ///
    /// `Ok(())` means the caller should recheck its own condition.
    pub(super) fn wait(
        cursor: &AtomicUsize,
        expected: SequenceNumber,
        deadline: Option<Instant>,
    ) -> Result<(), WaitError> {
        loop {
            if cursor.load(Ordering::Acquire) != expected {
                return Ok(());
            }

            if let Some(deadline) = deadline {
                remaining_until(deadline)?;
            }

            std::thread::yield_now();
        }
    }

    /// Polls until any source's word changes; see [`super::poll_any`].
    pub(super) fn wait_any<const N: usize>(
        sources: &[WaitSource<'_>; N],
        expected: &[SequenceNumber; N],
        deadline: Option<Instant>,
    ) -> Result<(), WaitError> {
        poll_any(sources, expected, deadline)
    }

    /// No-ops because spin waiters observe the shared cursor directly.
    pub(super) fn wake(_cursor: &AtomicUsize, _count: u32) {}
}

// Model the kernel's atomic compare-and-sleep operation. Timeouts and spurious
// wakes are omitted; threads remain asleep until explicitly woken.
#[cfg(loom)]
mod imp {
    use super::{SequenceNumber, WaitSource};
    use crate::{
        error::WaitError,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use loom::{sync::Mutex, thread};
    use std::time::Instant;

    /// Words a sleeper is still queued on, its thread, and whether a wake has
    /// already reached it.
    type Sleeper = (Vec<usize>, thread::Thread, bool);

    loom::lazy_static! {
        static ref SLEEPERS: Mutex<Vec<Sleeper>> = Mutex::new(Vec::new());
    }

    /// Number of sleepers still queued on any word.
    #[cfg(test)]
    pub(super) fn queued_sleepers() -> usize {
        SLEEPERS.lock().unwrap().len()
    }

    pub(super) fn wait(
        cursor: &AtomicUsize,
        expected: SequenceNumber,
        deadline: Option<Instant>,
    ) -> Result<(), WaitError> {
        assert!(
            deadline.is_none(),
            "Loom futex model does not model timeouts"
        );
        {
            // Serialize the comparison and sleeper registration against wake.
            let mut sleepers = SLEEPERS.lock().unwrap();
            if cursor.load(Ordering::Relaxed) as u32 != expected as u32 {
                return Ok(());
            }
            sleepers.push((
                vec![cursor as *const AtomicUsize as usize],
                thread::current(),
                false,
            ));
        }
        // Unpark retains a token if wake happens between registration and park.
        thread::park();
        Ok(())
    }

    /// Models `futex_waitv`: a sleeper may wait on several cursors at once.
    ///
    /// As in the kernel, a wake unqueues it only from the woken cursor; it
    /// stays queued on its other cursors, still counting toward wakes issued
    /// there, until it runs and unqueues itself.
    pub(super) fn wait_any<const N: usize>(
        sources: &[WaitSource<'_>; N],
        expected: &[SequenceNumber; N],
        deadline: Option<Instant>,
    ) -> Result<(), WaitError> {
        assert!(
            deadline.is_none(),
            "Loom futex model does not model timeouts"
        );
        {
            // Serialize the comparison and sleeper registration against wake.
            let mut sleepers = SLEEPERS.lock().unwrap();
            // Like the kernel: a plain 32-bit compare, no synchronization.
            let changed = sources.iter().zip(expected).any(|(source, &expected)| {
                source.word.load(Ordering::Relaxed) as u32 != expected as u32
            });
            if changed {
                return Ok(());
            }
            let addresses = sources
                .iter()
                .map(|source| source.word as *const AtomicUsize as usize)
                .collect();
            sleepers.push((addresses, thread::current(), false));
        }
        // Unpark retains a token if wake happens between registration and park.
        thread::park();
        // Like `futex_unqueue_multiple`: once running, leave the words that
        // did not wake this thread.
        let current = thread::current().id();
        SLEEPERS
            .lock()
            .unwrap()
            .retain(|(_, sleeper, _)| sleeper.id() != current);
        Ok(())
    }

    pub(super) fn wake(cursor: &AtomicUsize, mut count: u32) {
        let address = cursor as *const AtomicUsize as usize;
        let mut sleepers = SLEEPERS.lock().unwrap();
        let mut index = 0;
        while index < sleepers.len() && count > 0 {
            let (addresses, sleeper, woken) = &mut sleepers[index];
            if let Some(position) = addresses.iter().position(|&queued| queued == address) {
                // Unqueue from this word only. A sleeper already woken via
                // another word still consumes one of `count`, as in the kernel.
                addresses.swap_remove(position);
                if !*woken {
                    *woken = true;
                    sleeper.unpark();
                }
                count -= 1;
                if addresses.is_empty() {
                    sleepers.swap_remove(index);
                    continue;
                }
            }
            index += 1;
        }
    }
}

#[cfg(all(test, loom))]
mod loom_tests;

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::{sync::Arc, thread};

    #[derive(Default)]
    struct Source {
        waiters: Waiters,
        word: AtomicUsize,
    }

    impl Source {
        fn publish(&self) {
            self.word.fetch_add(1, Ordering::Release);
            self.waiters.wake(&self.word, 1);
        }

        fn wait_source(&self) -> WaitSource<'_> {
            WaitSource::new(&self.waiters, &self.word)
        }
    }

    fn ready(sources: &[Arc<Source>; 2]) -> Option<usize> {
        sources
            .iter()
            .position(|source| source.word.load(Ordering::Acquire) != 0)
    }

    #[test]
    fn wait_for_any_wakes_on_publication_to_any_source() {
        for target in 0..2 {
            let sources = [Arc::new(Source::default()), Arc::new(Source::default())];
            let publisher = {
                let source = sources[target].clone();
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(50));
                    source.publish();
                })
            };

            let wait_sources = [sources[0].wait_source(), sources[1].wait_source()];
            let woken = Waiters::wait_for_any(&wait_sources, 0, Duration::from_secs(10), || {
                ready(&sources)
            });
            assert_eq!(woken, Ok(target));
            publisher.join().unwrap();
            for source in &sources {
                assert_eq!(source.waiters.waiters.load(Ordering::Relaxed), 0);
            }
        }
    }

    #[test]
    fn wait_for_any_times_out_and_unregisters() {
        let sources = [Arc::new(Source::default()), Arc::new(Source::default())];
        let wait_sources = [sources[0].wait_source(), sources[1].wait_source()];
        let timeout = Duration::from_millis(50);
        let start = Instant::now();
        let result = Waiters::wait_for_any(&wait_sources, 0, timeout, || ready(&sources));
        assert_eq!(result, Err(WaitError::Timeout));
        assert!(start.elapsed() >= timeout);
        for source in &sources {
            assert_eq!(source.waiters.waiters.load(Ordering::Relaxed), 0);
        }
    }
}
