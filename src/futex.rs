//! Futex-backed waiting for the shared-memory queues.
//!
//! The futex word is the queue's own 64-bit publication cursor (the write
//! cursor for spsc, the producer publication cursor for mpmc) rather than a
//! dedicated sequence word. Every publish advances the cursor
//! unconditionally, so there is no conditional "bump the sequence only if
//! waiters are registered" step that a racing waiter could miss.
//!
//! # Lost-wake freedom
//!
//! The producer and consumer run a Dekker-style store/load protocol, which is
//! only correct if all four accesses participate in the SeqCst total order:
//!
//! - Producer (at each visibility point): publish the cursor with a SeqCst
//!   store (P1), then load `waiters` with SeqCst (P2); wake if non-zero.
//! - Consumer (in [`WaitState::wait_for`]): increment `waiters` with a SeqCst
//!   RMW (C1), then snapshot the cursor with a SeqCst load and recheck the
//!   condition (C2); sleep only if the condition still fails.
//!
//! In the SeqCst total order, either C1 precedes P2 — then P2 observes
//! `waiters >= 1` and the producer issues a wake — or P2 precedes C1, hence
//! P1 precedes C2, and C2 must observe the published cursor (a SeqCst load
//! ordered after a SeqCst store to the same location cannot read an older
//! value): the consumer's recheck sees the publication and never sleeps, or
//! `FUTEX_WAIT`'s in-kernel compare fails with `EAGAIN`. At least one arm
//! always holds, so the lost-wake cycle (producer skips the wake *and* the
//! consumer sleeps past the publication) is impossible. Acquire/Release is
//! not enough here: it permits both P2 and C2 to read stale values
//! (StoreLoad reordering), which is exactly the lost-wake interleaving.
//!
//! # Mixed-width access to the cursor
//!
//! All userspace accesses to the cursor are full-width 64-bit atomics; only
//! the kernel's `FUTEX_WAIT` compare reads 32 bits. The futex address is the
//! low half of the cursor — the half that changes on every publication — at
//! byte offset 0 on little-endian targets and 4 on big-endian targets. It is
//! 4-byte aligned either way, and an aligned 4-byte read cannot tear against
//! aligned 64-bit atomic writes. Never materialize an `&AtomicU32` into the
//! cursor in Rust code — only a raw pointer passed to the syscall.
//!
//! ABA caveat: if the cursor advances by an exact multiple of 2^32 between
//! the consumer's snapshot and the kernel's compare, the wait sleeps despite
//! progress. This is bounded by the caller's timeout and astronomically
//! unlikely in practice.
//!
//! The futex syscalls deliberately do not use `FUTEX_PRIVATE_FLAG`: the
//! queues live in shared memory mapped by multiple processes.

use crate::{error::WaitError, CacheAlignedAtomicU32};
use core::{
    hint::spin_loop,
    sync::atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

/// Snapshot of a queue's 64-bit publication cursor.
type SequenceNumber = usize;

fn deadline_from_timeout(timeout: Duration) -> Instant {
    Instant::now()
        .checked_add(timeout)
        .expect("timeout duration overflowed Instant")
}

fn remaining_until(deadline: Instant) -> Result<Duration, WaitError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(WaitError::Timeout)
}

#[repr(C)]
pub(crate) struct WaitState {
    /// Approximate count of waiters registered against the queue's
    /// publication cursor.
    waiters: CacheAlignedAtomicU32,
}

impl WaitState {
    /// Initializes this wait state inside a newly created shared-memory header.
    pub(crate) fn initialize(&self) {
        self.waiters.store(0, Ordering::Release);
    }

    /// Runs `check` until it returns a value or `timeout` elapses.
    ///
    /// `cursor` is the queue's publication cursor used as the futex word
    /// while sleeping; an advance of `cursor` must imply that `check` can
    /// observe new data. `spin_attempts` controls how many extra checks run
    /// before registering as a waiter and entering the platform wait backend.
    pub(crate) fn wait_for<T>(
        &self,
        cursor: &AtomicUsize,
        timeout: Duration,
        spin_attempts: usize,
        mut check: impl FnMut() -> Option<T>,
    ) -> Result<T, WaitError> {
        let deadline = deadline_from_timeout(timeout);
        loop {
            if let Some(value) = check() {
                return Ok(value);
            }

            for _ in 0..spin_attempts {
                spin_loop();
                if let Some(value) = check() {
                    return Ok(value);
                }
            }

            let snapshot = self.register(cursor);
            // Recheck after registering because a producer can publish after
            // the unregistered check and before this thread starts waiting.
            // This is C2 in the module-level ordering argument.
            if let Some(value) = check() {
                self.unregister();
                return Ok(value);
            }

            // Platform waits can return after a matching wake or a spurious
            // wake, so success only means the caller's condition should be
            // checked again at the top of the loop.
            let wait_result = Self::wait(cursor, snapshot, deadline);
            self.unregister();
            wait_result?;
        }
    }

    /// Registers this thread as a waiter and snapshots the cursor.
    ///
    /// The SeqCst RMW (C1) followed by the SeqCst cursor load are the
    /// consumer half of the module-level ordering argument.
    fn register(&self, cursor: &AtomicUsize) -> SequenceNumber {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        cursor.load(Ordering::SeqCst)
    }

    fn unregister(&self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }

    fn wait(
        cursor: &AtomicUsize,
        expected: SequenceNumber,
        deadline: Instant,
    ) -> Result<(), WaitError> {
        // Avoid entering the platform wait backend if a publication already
        // advanced the cursor after the caller's post-registration recheck.
        if cursor.load(Ordering::SeqCst) != expected {
            return Ok(());
        }

        imp::wait(cursor, expected, deadline)
    }

    /// Wakes up to `count` waiters registered against `cursor`.
    ///
    /// Callers must publish `cursor` with a SeqCst (full-barrier) operation
    /// before calling this (P1 in the module-level ordering argument). The
    /// publication itself must be unconditional — only the wake syscall is
    /// elided when no waiter is registered.
    pub(crate) fn wake(&self, cursor: &AtomicUsize, count: usize) {
        debug_assert!(count > 0);

        // P2 in the module-level ordering argument. A plain load keeps the
        // hot publish path free of RMWs on a line consumers rarely write.
        let waiters = self.waiters.load(Ordering::SeqCst);
        if waiters == 0 {
            return;
        }

        let count = waiters.min(count.min(MAX_WAKE_COUNT as usize) as u32);
        imp::wake(cursor, count);
    }
}

const MAX_WAKE_COUNT: u32 = i32::MAX as u32;

#[cfg(target_os = "linux")]
mod imp {
    use super::{remaining_until, SequenceNumber};
    use crate::error::WaitError;
    use core::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    /// Returns the futex word: the low 32 bits of the 64-bit cursor, the
    /// half that changes on every publication.
    ///
    /// Only the kernel reads through this pointer; never materialize an
    /// `&AtomicU32` over the cursor in Rust code (see module docs).
    fn futex_word(cursor: &AtomicUsize) -> *mut u32 {
        let ptr = cursor.as_ptr().cast::<u32>();
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
        deadline: Instant,
    ) -> Result<(), WaitError> {
        let expected = expected as u32;
        loop {
            let remaining = remaining_until(deadline)?;

            let timeout_storage = duration_to_timespec(remaining);
            let timeout_ptr = &timeout_storage as *const libc::timespec;

            // SAFETY:
            // - `futex_word(cursor)` is a valid, 4-byte aligned pointer into a
            //   live 64-bit atomic; the kernel only reads 32 bits through it.
            // - `timeout_ptr` points to a live `timespec`.
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
        debug_assert!(duration.as_secs() <= libc::time_t::MAX as u64);
        libc::timespec {
            tv_sec: duration.as_secs() as libc::time_t,
            tv_nsec: duration.subsec_nanos() as libc::c_long,
        }
    }

    fn errno() -> i32 {
        // SAFETY: `__errno_location` returns this thread's errno location on Linux.
        unsafe { *libc::__errno_location() }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::{remaining_until, SequenceNumber};
    use crate::error::WaitError;
    use core::{
        hint::spin_loop,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use std::time::Instant;

    /// Busy-waits until `cursor` no longer equals `expected` or timeout elapses.
    ///
    /// `Ok(())` means the caller should recheck its own condition.
    pub(super) fn wait(
        cursor: &AtomicUsize,
        expected: SequenceNumber,
        deadline: Instant,
    ) -> Result<(), WaitError> {
        loop {
            if cursor.load(Ordering::SeqCst) != expected {
                return Ok(());
            }

            remaining_until(deadline)?;

            spin_loop();
        }
    }

    /// No-ops because spin waiters observe the shared cursor directly.
    pub(super) fn wake(_cursor: &AtomicUsize, _count: u32) {}
}
