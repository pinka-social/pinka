use anyhow::Result;
use fjall::{Batch, Keyspace, PartitionCreateOptions, PartitionHandle, UserKey};
use uuid::Uuid;

use crate::activity_pub::model::{Actor, Object};

use super::{ObjectKey, ObjectRepo, make_object_key, uuidgen};

struct FollowerKey {
    uid: String,
    sort_key: Uuid,
}

impl FollowerKey {
    fn new(uid: String) -> FollowerKey {
        FollowerKey {
            uid,
            sort_key: uuidgen(),
        }
    }
}

impl From<FollowerKey> for UserKey {
    fn from(value: FollowerKey) -> Self {
        let mut key = vec![];
        key.extend_from_slice(value.uid.as_bytes());
        key.extend_from_slice(value.sort_key.as_bytes());
        key.into()
    }
}

#[derive(Clone)]
pub(crate) struct UserIndex {
    object_repo: ObjectRepo,
    user_index: PartitionHandle,
    follower_index: PartitionHandle,
}

impl UserIndex {
    pub(crate) fn new(keyspace: Keyspace) -> Result<UserIndex> {
        let object_repo = ObjectRepo::new(keyspace.clone())?;
        let user_index =
            keyspace.open_partition("user_index", PartitionCreateOptions::default())?;
        let follower_index =
            keyspace.open_partition("follower_index", PartitionCreateOptions::default())?;
        Ok(UserIndex {
            object_repo,
            user_index,
            follower_index,
        })
    }
    pub(crate) fn insert(&self, b: &mut Batch, uid: String, user: Actor) -> Result<()> {
        let obj_key = make_object_key();
        self.object_repo.insert(b, obj_key, user)?;
        b.insert(&self.user_index, uid, obj_key);
        Ok(())
    }
    pub(crate) fn insert_follower(&self, b: &mut Batch, uid: String, key: ObjectKey) -> Result<()> {
        let follower_key = FollowerKey::new(uid);
        b.insert(&self.follower_index, follower_key, key);
        Ok(())
    }
    pub(crate) fn find_one(&self, uid: String) -> Result<Option<Object>> {
        if let Some(key) = self.user_index.get(uid)? {
            return Ok(self.object_repo.find_one(key)?);
        }
        Ok(None)
    }
    pub(crate) fn find_followers(&self, uid: String) -> Result<Vec<UserKey>> {
        let mut result = vec![];
        for pair in self.follower_index.prefix(uid) {
            let (_, obj_key) = pair?;
            result.push(obj_key);
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

    use crate::activity_pub::model::Object;

    use super::{Actor, UserIndex};

    #[test]
    fn insert_then_find() -> Result<()> {
        let tmp_dir = tempdir()?;
        let keyspace = Keyspace::open(Config::new(tmp_dir.path()).temporary(true))?;
        let mut b = keyspace.batch();
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
        repo.insert(&mut b, "kenzoishii".to_string(), actor.clone())?;
        b.commit()?;
        assert_eq!(Some(obj), repo.find_one("kenzoishii".to_string())?);
        Ok(())
    }
}
