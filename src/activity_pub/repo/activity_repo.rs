use anyhow::Result;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle};

use crate::activity_pub::model::Object;
use crate::activity_pub::object_serde::ObjectSerDe;

#[derive(Clone)]
pub(crate) struct ActivityRepo {
    acts: PartitionHandle,
}

impl ActivityRepo {
    pub(crate) fn new(keyspace: Keyspace) -> Result<ActivityRepo> {
        let acts = keyspace.open_partition("acts", PartitionCreateOptions::default())?;
        Ok(ActivityRepo { acts })
    }
    pub(crate) fn insert(&self, iri: &str, object: Object) -> Result<()> {
        let bytes = object.into_bytes()?;
        self.acts.insert(iri, bytes)?;
        Ok(())
    }
    pub(crate) fn find_one(&self, iri: &str) -> Result<Option<Object>> {
        if let Some(bytes) = self.acts.get(iri)? {
            let object = Object::from_bytes(&bytes)?;
            return Ok(Some(object));
        }
        Ok(None)
    }
    pub(crate) fn all(&self) -> Result<Vec<Object>> {
        let mut result = vec![];
        for bytes in self.acts.values() {
            let object = Object::from_bytes(&bytes?)?;
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

    use crate::activity_pub::model::Create;

    use super::{ActivityRepo, Object};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let keyspace = Keyspace::open(Config::new(tmp_dir.path()).temporary(true))?;
        let repo = ActivityRepo::new(keyspace)?;
        let object = Object::try_from(json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Create",
            "id": "https://example.net/~mallory/87374",
            "actor": "https://example.net/~mallory",
            "object": {
              "id": "https://example.com/~mallory/note/72",
              "type": "Note",
              "attributedTo": "https://example.net/~mallory",
              "content": "This is a note",
              "published": "2015-02-10T15:04:55Z",
              "to": ["https://example.org/~john/"],
              "cc": ["https://example.com/~erik/followers",
                     "https://www.w3.org/ns/activitystreams#Public"]
            },
            "published": "2015-02-10T15:04:55Z",
            "to": ["https://example.org/~john/"],
            "cc": ["https://example.com/~erik/followers",
                   "https://www.w3.org/ns/activitystreams#Public"]
        }))?;
        let create = Create::try_from(object.clone())?;
        let iri = "https://example.com/~mallory/note/72";
        repo.insert(iri, create.into())?;
        assert_eq!(Some(object), repo.find_one(iri)?);
        Ok(())
    }
}
