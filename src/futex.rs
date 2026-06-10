fn deadline_from_timeout(timeout: std::time::Duration) -> std::time::Instant {
    std::time::Instant::now()
        .checked_add(timeout)
        .expect("timeout duration overflowed Instant")
}

fn remaining_until(
    deadline: std::time::Instant,
) -> Result<std::time::Duration, crate::error::WaitError> {
    deadline
        .checked_duration_since(std::time::Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(crate::error::WaitError::Timeout)
}

#[repr(C)]
pub(crate) struct WaitState {
    /// Sequence word observed by waiters and advanced by wakers.
    sequence: crate::CacheAlignedAtomicU32,
    /// Approximate count of waiters registered against `sequence`.
    waiters: crate::CacheAlignedAtomicU32,
}

impl WaitState {
    /// Initializes this wait state inside a newly created shared-memory header.
    pub(crate) fn initialize(&self) {
        self.sequence
            .store(0, core::sync::atomic::Ordering::Release);
        self.waiters.store(0, core::sync::atomic::Ordering::Release);
    }

    /// Runs `check` until it returns a value or `timeout` elapses.
    ///
    /// `spin_attempts` controls how many extra checks run before registering as
    /// a waiter and entering the platform wait backend.
    pub(crate) fn wait_for<T>(
        &self,
        timeout: std::time::Duration,
        spin_attempts: usize,
        mut check: impl FnMut() -> Option<T>,
    ) -> Result<T, crate::error::WaitError> {
        let deadline = deadline_from_timeout(timeout);
        loop {
            if let Some(value) = check() {
                return Ok(value);
            }

            for _ in 0..spin_attempts {
                core::hint::spin_loop();
                if let Some(value) = check() {
                    return Ok(value);
                }
            }

            let sequence = self.register();
            // Recheck after registering because a producer can publish after
            // the unregistered check and before this thread starts waiting.
            if let Some(value) = check() {
                self.unregister();
                return Ok(value);
            }

            // Platform waits can return after a matching wake or a spurious
            // wake, so success only means the caller's condition should be
            // checked again at the top of the loop.
            let wait_result = self.wait(sequence, deadline);
            self.unregister();
            wait_result?;
        }
    }

    fn register(&self) -> u32 {
        self.waiters
            .fetch_add(1, core::sync::atomic::Ordering::AcqRel);
        self.sequence.load(core::sync::atomic::Ordering::Acquire)
    }

    fn unregister(&self) {
        self.waiters
            .fetch_sub(1, core::sync::atomic::Ordering::AcqRel);
    }

    fn wait(
        &self,
        expected: u32,
        deadline: std::time::Instant,
    ) -> Result<(), crate::error::WaitError> {
        // Avoid entering the platform wait backend if a wake already advanced
        // the sequence after the caller's post-registration recheck.
        if self.sequence.load(core::sync::atomic::Ordering::Acquire) != expected {
            return Ok(());
        }

        imp::wait(&self.sequence, expected, deadline)
    }

    /// Wakes up to `count` registered waiters.
    pub(crate) fn wake(&self, count: usize) {
        debug_assert!(count > 0);

        let waiters = self.waiters.load(core::sync::atomic::Ordering::Acquire);
        if waiters == 0 {
            return;
        }

        self.sequence
            .fetch_add(1, core::sync::atomic::Ordering::Release);
        let count = waiters.min(count.min(MAX_WAKE_COUNT as usize) as u32);
        imp::wake(&self.sequence, count);
    }
}

const MAX_WAKE_COUNT: u32 = i32::MAX as u32;

#[cfg(target_os = "linux")]
mod imp {
    /// Blocks with Linux `FUTEX_WAIT` while `futex` still equals `expected`.
    ///
    /// `Ok(())` means the caller should recheck its own condition; Linux can
    /// return success for ordinary wakes and for spurious wakes.
    pub(super) fn wait(
        futex: &core::sync::atomic::AtomicU32,
        expected: u32,
        deadline: std::time::Instant,
    ) -> Result<(), crate::error::WaitError> {
        loop {
            let remaining = super::remaining_until(deadline)?;

            let timeout_storage = duration_to_timespec(remaining);
            let timeout_ptr = &timeout_storage as *const libc::timespec;

            // SAFETY:
            // - `futex.as_ptr()` is a valid pointer to a 4-byte aligned atomic.
            // - `timeout_ptr` points to a live `timespec`.
            // - `FUTEX_WAIT` only blocks if the value still equals `expected`.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    futex.as_ptr(),
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
                libc::ETIMEDOUT => return Err(crate::error::WaitError::Timeout),
                err => panic!("unexpected futex wait error: errno={err}"),
            }
        }
    }

    /// Wakes waiters blocked in Linux `FUTEX_WAIT` on `futex`.
    pub(super) fn wake(futex: &core::sync::atomic::AtomicU32, count: u32) {
        debug_assert!(count <= libc::c_int::MAX as u32);

        // SAFETY: `futex.as_ptr()` is a valid pointer to a 4-byte aligned atomic.
        let result = unsafe {
            libc::syscall(
                libc::SYS_futex,
                futex.as_ptr(),
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

    /// Converts a [`std::time::Duration`] to the relative [`libc::timespec`]
    /// timeout format expected by futex.
    #[inline]
    const fn duration_to_timespec(duration: std::time::Duration) -> libc::timespec {
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
    /// Busy-waits until `futex` no longer equals `expected` or timeout elapses.
    ///
    /// `Ok(())` means the caller should recheck its own condition.
    pub(super) fn wait(
        futex: &core::sync::atomic::AtomicU32,
        expected: u32,
        deadline: std::time::Instant,
    ) -> Result<(), crate::error::WaitError> {
        loop {
            if futex.load(core::sync::atomic::Ordering::Acquire) != expected {
                return Ok(());
            }

            super::remaining_until(deadline)?;

            core::hint::spin_loop();
        }
    }

    /// No-ops because spin waiters observe the shared sequence word directly.
    pub(super) fn wake(_futex: &core::sync::atomic::AtomicU32, _count: u32) {}
}
