use anyhow::Result;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle};

use crate::activity_pub::model::Actor;
use crate::activity_pub::object_serde::ObjectSerDe;

#[derive(Clone)]
pub(crate) struct ActorRepo {
    actors: PartitionHandle,
}

impl ActorRepo {
    pub(crate) fn new(keyspace: Keyspace) -> Result<ActorRepo> {
        let actors = keyspace.open_partition("actors", PartitionCreateOptions::default())?;
        Ok(ActorRepo { actors })
    }
    pub(crate) fn insert(&self, iri: &str, actor: Actor) -> Result<()> {
        let bytes = actor.into_bytes()?;
        self.actors.insert(iri, bytes)?;
        Ok(())
    }
    pub(crate) fn find_one(&self, iri: &str) -> Result<Option<Actor>> {
        if let Some(bytes) = self.actors.get(iri)? {
            let actor = Actor::from_bytes(&bytes)?;
            return Ok(Some(actor));
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

    use super::{Actor, ActorRepo};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let keyspace = Keyspace::open(Config::new(tmp_dir.path()).temporary(true))?;
        let store = ActorRepo::new(keyspace)?;
        let actor = Actor::try_from(json!(
            {
                "@context": ["https://www.w3.org/ns/activitystreams",
                             {"@language": "ja"}],
                "type": "Person",
                "id": "https://kenzoishii.example.com/",
                "preferredUsername": "kenzoishii",
                "name": "石井健蔵",
                "summary": "この方はただの例です",
                "icon": [
                  "https://kenzoishii.example.com/image/165987aklre4"
                ]
              }
        ))?;
        let iri = "https://kenzoishii.example.com/";
        store.insert(iri, actor.clone())?;
        assert_eq!(Some(actor), store.find_one(iri)?);
        Ok(())
    }
}
