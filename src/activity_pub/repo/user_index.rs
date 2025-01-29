use anyhow::Result;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle};
use uuid::Uuid;

use crate::activity_pub::model::{Actor, Object};

use super::{ObjectRepo, make_object_key};

#[derive(Clone)]
pub(crate) struct UserIndex {
    keyspace: Keyspace,
    object_repo: ObjectRepo,
    user_index: PartitionHandle,
}

impl UserIndex {
    pub(crate) fn new(keyspace: Keyspace) -> Result<UserIndex> {
        let object_repo = ObjectRepo::new(keyspace.clone())?;
        let user_index =
            keyspace.open_partition("user_index", PartitionCreateOptions::default())?;
        Ok(UserIndex {
            keyspace,
            object_repo,
            user_index,
        })
    }
    pub(crate) fn insert(&self, uid: String, user: Actor) -> Result<()> {
        let mut batch = self.keyspace.batch();
        let obj_key = make_object_key();
        self.object_repo.batch_insert(&mut batch, obj_key, user)?;
        batch.insert(&self.user_index, uid, obj_key);
        batch.commit()?;
        Ok(())
    }
    pub(crate) fn find_one(&self, uid: String) -> Result<Option<Object>> {
        if let Some(key) = self.user_index.get(uid)? {
            return Ok(self.object_repo.find_one(key)?);
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

    use crate::activity_pub::model::Object;

    use super::{Actor, UserIndex};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let keyspace = Keyspace::open(Config::new(tmp_dir.path()).temporary(true))?;
        let repo = UserIndex::new(keyspace)?;
        let obj = Object::try_from(json!(
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
        let actor = Actor::try_from(obj.clone())?;
        repo.insert("kenzoishii".to_string(), actor.clone())?;
        assert_eq!(Some(obj), repo.find_one("kenzoishii".to_string())?);
        Ok(())
    }
}
