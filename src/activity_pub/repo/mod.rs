mod actor_repo;

pub(crate) use actor_repo::ActorRepo;

pub(super) fn base62_uuid() -> String {
    let uuid_7 = uuid::Uuid::now_v7();
    base62::encode(uuid_7.as_u128())
}
