//! Bounded, coalescing delivery scheduling independent of transports and schemas.
#![forbid(unsafe_code)]
pub mod queue;
mod mailbox;
pub use mailbox::{Delivery, Error, Kind, Limits, Mailbox, Message, Offer, Progress, Receipt};
