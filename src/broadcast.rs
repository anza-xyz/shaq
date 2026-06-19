//! Multi-producer / multi-consumer broadcast queue.
//!
//! Each producer owns a [`producer_lane::ProducerLane`]; every consumer reads
//! every lane. The producer/consumer handles and the shared region wiring land
//! in later commits.

mod producer_lane;
