use anyhow::Result;
use fjall::{Batch, Keyspace, KvSeparationOptions, PartitionCreateOptions, PartitionHandle};
use serde_json::Value;

use crate::activity_pub::model::Object;
use crate::activity_pub::object_serde;

use super::ObjectKey;

#[derive(Clone)]
pub(crate) struct ObjectRepo {
    objects: PartitionHandle,
}

impl ObjectRepo {
    pub(crate) fn new(keyspace: Keyspace) -> Result<ObjectRepo> {
        let objects = keyspace.open_partition(
            "objects",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;
        Ok(ObjectRepo { objects })
    }
    pub(crate) fn insert(
        &self,
        b: &mut Batch,
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
    pub(crate) fn all(&self) -> Result<Vec<Object<'static>>> {
        let mut result = vec![];
        for bytes in self.objects.values() {
            let object = object_serde::from_bytes(&bytes?)?;
            result.push(object);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use fjall::{Config, Keyspace};
    use serde_json::json;
    use tempfile::tempdir;

    use super::{Object, ObjectKey, ObjectRepo};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let keyspace = Keyspace::open(Config::new(tmp_dir.path()).temporary(true))?;
        let repo = ObjectRepo::new(keyspace.clone())?;
        let object = Object::try_from(json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Note",
            "content": "This is a note",
            "published": "2015-02-10T15:04:55Z",
            "to": ["https://example.org/~john/"],
            "cc": ["https://example.com/~erik/followers",
                "https://www.w3.org/ns/activitystreams#Public"]
        }))?;
        let mut b = keyspace.batch();
        let obj_key = ObjectKey::new();
        repo.insert(&mut b, obj_key, object.clone())?;
        b.commit()?;
        assert_eq!(Some(object), repo.find_one(obj_key)?);
        Ok(())
    }
}
