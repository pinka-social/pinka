#[macro_use]
mod object_serde;
mod repo;

pub(crate) mod machine;
pub(crate) mod model;

pub(crate) use repo::ActorRepo;
