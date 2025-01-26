mod activity_repo;
mod actor_repo;
mod object_repo;

pub(crate) use activity_repo::ActivityRepo;
pub(crate) use actor_repo::ActorRepo;
pub(crate) use object_repo::ObjectRepo;

pub(super) fn base62_uuid() -> String {
    let uuid_7 = uuid::Uuid::now_v7();
    base62::encode(uuid_7.as_u128())
}
