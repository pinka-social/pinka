use anyhow::Result;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, OwnedWriteBatch};
use serde_json::Value;

use crate::activity_pub::model::Object;
use crate::activity_pub::object_serde;

use super::ObjectKey;

#[derive(Clone)]
pub(crate) struct ObjectRepo {
    objects: Keyspace,
}

impl ObjectRepo {
    pub(crate) fn new(database: Database) -> Result<ObjectRepo> {
        let objects = database.keyspace("objects", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;
        Ok(ObjectRepo { objects })
    }
    pub(crate) fn insert(
        &self,
        b: &mut OwnedWriteBatch,
        key: ObjectKey,
        object: impl Into<Value>,
    ) -> Result<()> {
        let bytes = object_serde::to_bytes(object)?;
        b.insert(&self.objects, key, bytes);
        Ok(())
    }
    pub(crate) fn find_one(&self, key: impl AsRef<[u8]>) -> Result<Option<Object<'static>>> {
        if let Some(bytes) = self.objects.get(key)? {
            let object = object_serde::from_bytes(&bytes)?;
            return Ok(Some(object));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use fjall::Database;
    use serde_json::json;
    use tempfile::tempdir;

    use super::{Object, ObjectKey, ObjectRepo};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let database = Database::builder(tmp_dir.path()).temporary(true).open()?;
        let repo = ObjectRepo::new(database.clone())?;
        let object = Object::try_from(json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Note",
            "content": "This is a note",
            "published": "2015-02-10T15:04:55Z",
            "to": ["https://example.org/~john/"],
            "cc": ["https://example.com/~erik/followers",
                "https://www.w3.org/ns/activitystreams#Public"]
        }))?;
        let mut b = database.batch();
        let obj_key = ObjectKey::new();
        repo.insert(&mut b, obj_key, object.clone())?;
        b.commit()?;
        assert_eq!(Some(object), repo.find_one(obj_key)?);
        Ok(())
    }
}
