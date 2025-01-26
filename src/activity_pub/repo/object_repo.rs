use anyhow::Result;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle};

use crate::activity_pub::model::Object;
use crate::activity_pub::object_serde::ObjectSerDe;

#[derive(Clone)]
pub(crate) struct ObjectRepo {
    objects: PartitionHandle,
}

impl ObjectRepo {
    pub(crate) fn new(keyspace: Keyspace) -> Result<ObjectRepo> {
        let objects = keyspace.open_partition("objects", PartitionCreateOptions::default())?;
        Ok(ObjectRepo { objects })
    }
    pub(crate) fn insert(&self, iri: &str, object: Object) -> Result<()> {
        let bytes = object.into_bytes()?;
        self.objects.insert(iri, bytes)?;
        Ok(())
    }
    pub(crate) fn find_one(&self, iri: &str) -> Result<Option<Object>> {
        if let Some(bytes) = self.objects.get(iri)? {
            let object = Object::from_bytes(&bytes)?;
            return Ok(Some(object));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use fjall::{Config, Keyspace};
    use serde_json::json;
    use tempfile::tempdir;

    use super::{Object, ObjectRepo};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let keyspace = Keyspace::open(Config::new(tmp_dir.path()).temporary(true))?;
        let repo = ObjectRepo::new(keyspace)?;
        let object = Object::try_from(json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Note",
            "content": "This is a note",
            "published": "2015-02-10T15:04:55Z",
            "to": ["https://example.org/~john/"],
            "cc": ["https://example.com/~erik/followers",
                "https://www.w3.org/ns/activitystreams#Public"]
        }))?;
        let iri = "https://example.com/~mallory/note/72";
        repo.insert(iri, object.clone())?;
        assert_eq!(Some(object), repo.find_one(iri)?);
        Ok(())
    }
}
