#[macro_use]
mod object_serde;
mod repo;

pub(crate) mod machine;
pub(crate) mod model;

pub(crate) use repo::OutboxIndex;
pub(crate) use repo::UserIndex;
pub(crate) use repo::{ObjectKey, ObjectRepo};
