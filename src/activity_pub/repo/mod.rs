mod context_index;
mod object_repo;
mod outbox_index;
mod user_index;

pub(crate) use context_index::ContextIndex;
pub(crate) use object_repo::ObjectRepo;
pub(crate) use outbox_index::OutboxIndex;
pub(crate) use user_index::UserIndex;

use fjall::UserKey;
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(crate) struct ObjectKey(Uuid);

impl From<ObjectKey> for UserKey {
    fn from(value: ObjectKey) -> Self {
        UserKey::new(value.0.as_bytes())
    }
}

impl AsRef<[u8]> for ObjectKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

pub(super) fn base62_uuid() -> String {
    let uuid_7 = uuid::Uuid::now_v7();
    base62::encode(uuid_7.as_u128())
}

pub(super) fn uuidgen() -> Uuid {
    uuid::Uuid::now_v7()
}

pub(super) fn make_object_key() -> ObjectKey {
    ObjectKey(uuidgen())
}
