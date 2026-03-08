use anyhow::Result;
use fjall::{Database, Keyspace, OwnedWriteBatch};

use crate::activity_pub::model::{Actor, Object};

use super::xindex::IdObjIndex;
use super::{IdObjIndexKey, ObjectKey, ObjectRepo};

#[derive(Clone)]
pub(crate) struct UserIndex {
    object_repo: ObjectRepo,
    user_index: Keyspace,
    follower_index: IdObjIndex,
}

impl UserIndex {
    pub(crate) fn new(database: Database) -> Result<UserIndex> {
        let object_repo = ObjectRepo::new(database.clone())?;
        let user_index = database.keyspace("user_index", || Default::default())?;
        let follower_index =
            IdObjIndex::new(database.keyspace("follower_index", || Default::default())?);
        Ok(UserIndex {
            object_repo,
            user_index,
            follower_index,
        })
    }
    pub(crate) fn insert(&self, b: &mut OwnedWriteBatch, uid: &str, user: Actor) -> Result<()> {
        // FIXME
        let obj_key = ObjectKey::new();
        self.object_repo.insert(b, obj_key, user)?;
        b.insert(&self.user_index, uid, obj_key);
        Ok(())
    }
    pub(crate) fn insert_follower(&self, b: &mut OwnedWriteBatch, uid: &str, key: ObjectKey) {
        self.follower_index.insert(b, IdObjIndexKey::new(uid, key))
    }
    pub(crate) fn remove_follower(&self, b: &mut OwnedWriteBatch, uid: &str, key: ObjectKey) {
        self.follower_index.remove(b, IdObjIndexKey::new(uid, key))
    }
    pub(crate) fn find_one(&self, uid: &str) -> Result<Option<Object<'_>>> {
        if let Some(key) = self.user_index.get(uid)? {
            return self.object_repo.find_one(key);
        }
        Ok(None)
    }
    pub(crate) fn count_followers(&self, uid: &str) -> u64 {
        self.follower_index.count(uid)
    }
    pub(crate) fn find_followers(
        &self,
        uid: &str,
        before: Option<String>,
        after: Option<String>,
        first: Option<u64>,
        last: Option<u64>,
    ) -> Result<Vec<(ObjectKey, String)>> {
        let keys = self
            .follower_index
            .find_all(uid, before, after, first, last)?;
        let mut items = vec![];
        for key in keys {
            if let Some(obj) = self.object_repo.find_one(key.as_ref())? {
                items.push((
                    ObjectKey::try_from(key.as_ref())?,
                    obj.get_node_iri("actor")
                        .expect("actor should have id property")
                        .to_owned(),
                ));
            }
        }
        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use fjall::Database;
    use serde_json::json;
    use tempfile::tempdir;

    use crate::activity_pub::model::Object;

    use super::{Actor, UserIndex};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let database = Database::builder(tmp_dir.path()).temporary(true).open()?;
        let mut b = database.batch();
        let repo = UserIndex::new(database)?;
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
        repo.insert(&mut b, "kenzoishii", actor.clone())?;
        b.commit()?;
        assert_eq!(Some(obj), repo.find_one("kenzoishii")?);
        Ok(())
    }
}
