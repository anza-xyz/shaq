# shaq

[![Rust CI](https://github.com/anza-xyz/shaq/actions/workflows/ci.yml/badge.svg)](https://github.com/anza-xyz/shaq/actions/workflows/ci.yml)

shaq is a **SHAred Queue**: a simple shared-memory SPSC (Single Producer Single Consumer) and MPMC (Multi Producer Multi Consumer) FIFO queue.
It is designed for efficient inter-thread or inter-process communication using a lock-free, memory-mapped queue.
