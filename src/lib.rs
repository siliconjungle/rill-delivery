//! Bounded, coalescing delivery scheduling independent of transports and schemas.
#![forbid(unsafe_code)]
mod mailbox;
pub mod queue;
pub use mailbox::{Delivery, Error, Kind, Limits, Mailbox, Message, Offer, Progress, Receipt};
mod budget;
mod dispatcher;
pub use budget::TokenBucket;
pub use dispatcher::{DispatchBudget, DispatchStats, Dispatcher, Envelope, Peer, PeerConfig};
