//! Synchronization primitives selected for production or Loom model checking.

#[cfg(not(loom))]
pub(crate) use core::sync::atomic;

#[cfg(loom)]
pub(crate) use loom::sync::atomic;

#[cfg(not(loom))]
pub(crate) use std::sync::Arc;

#[cfg(loom)]
pub(crate) use loom::sync::Arc;
