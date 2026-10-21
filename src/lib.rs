//! Bounded, coalescing delivery scheduling independent of transports and schemas.
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]
mod mailbox;
pub mod queue;
pub use mailbox::{
    Delivery, Error, Kind, Limits, Mailbox, Message, Offer, Progress, Receipt, Stream,
};
mod budget;
mod dispatcher;
pub use budget::TokenBucket;
pub use dispatcher::{DispatchBudget, DispatchStats, Dispatcher, Envelope, Peer, PeerConfig};
