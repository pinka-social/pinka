#[macro_use]
mod object_serde;
mod hs2019;
mod mailman;
mod repo;
mod simple_queue;

pub(crate) mod delivery;
pub(crate) mod machine;
pub(crate) mod model;

pub(crate) use repo::ContextIndex;
pub(crate) use repo::CryptoRepo;
pub(crate) use repo::IriIndex;
pub(crate) use repo::OutboxIndex;
pub(crate) use repo::UserIndex;
pub(crate) use repo::{ObjectKey, ObjectRepo};

use uuid::Bytes;
use uuid::Uuid;

pub(crate) fn uuidgen() -> Bytes {
    Uuid::now_v7().into_bytes()
}
