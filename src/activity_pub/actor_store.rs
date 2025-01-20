use anyhow::Result;
use fjall::{PartitionCreateOptions, PartitionHandle};

use crate::config::RuntimeConfig;

use super::ObjectSerDe;
use super::actor::Actor;

#[derive(Clone)]
pub(crate) struct ActorStore {
    config: RuntimeConfig,
    actors: PartitionHandle,
}

impl ActorStore {
    pub(crate) fn new(config: RuntimeConfig) -> Result<ActorStore> {
        let actors = config
            .keyspace
            .open_partition("actors", PartitionCreateOptions::default())?;
        Ok(ActorStore { config, actors })
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

    use super::{Actor, ActorStore, RuntimeConfig};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let keyspace = Keyspace::open(Config::new(tmp_dir.path()).temporary(true))?;
        let config = RuntimeConfig {
            init: Default::default(),
            server: Default::default(),
            keyspace,
        };
        let store = ActorStore::new(config)?;
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
