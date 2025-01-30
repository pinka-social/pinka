use anyhow::Result;
use fjall::{Batch, Keyspace, PartitionCreateOptions, PartitionHandle, UserKey};
use uuid::Uuid;

use super::{ObjectKey, uuidgen};

struct ContextKey {
    iri: String,
    sort_key: Uuid,
}

impl ContextKey {
    fn new(iri: String) -> ContextKey {
        ContextKey {
            iri,
            sort_key: uuidgen(),
        }
    }
}

impl From<ContextKey> for UserKey {
    fn from(value: ContextKey) -> Self {
        let mut key = vec![];
        key.extend_from_slice(value.iri.as_bytes());
        key.extend_from_slice(value.sort_key.as_bytes());
        key.into()
    }
}

#[derive(Clone)]
pub(crate) struct ContextIndex {
    ctx_index: PartitionHandle,
    likes_index: PartitionHandle,
    shares_index: PartitionHandle,
}

impl ContextIndex {
    pub(crate) fn new(keyspace: Keyspace) -> Result<ContextIndex> {
        let options = PartitionCreateOptions::default();
        let ctx_index = keyspace.open_partition("ctx_index", options.clone())?;
        let likes_index = keyspace.open_partition("likes_index", options.clone())?;
        let shares_index = keyspace.open_partition("shares_index", options.clone())?;
        Ok(ContextIndex {
            ctx_index,
            likes_index,
            shares_index,
        })
    }
    pub(crate) fn insert(&self, b: &mut Batch, iri: String, obj_key: ObjectKey) -> Result<()> {
        let ctx_key = ContextKey::new(iri);
        b.insert(&self.ctx_index, ctx_key, obj_key);
        Ok(())
    }
    pub(crate) fn insert_likes(
        &self,
        b: &mut Batch,
        iri: String,
        obj_key: ObjectKey,
    ) -> Result<()> {
        let ctx_key = ContextKey::new(iri);
        b.insert(&self.ctx_index, ctx_key, obj_key);
        Ok(())
    }
    pub(crate) fn insert_shares(
        &self,
        b: &mut Batch,
        iri: String,
        obj_key: ObjectKey,
    ) -> Result<()> {
        let ctx_key = ContextKey::new(iri);
        b.insert(&self.ctx_index, ctx_key, obj_key);
        Ok(())
    }
    fn find_by_iri(&self, index: &PartitionHandle, iri: String) -> Result<Vec<UserKey>> {
        let mut result = vec![];
        for pair in self.ctx_index.prefix(iri) {
            let (_, obj_key) = pair?;
            result.push(obj_key);
        }
        Ok(result)
    }
    pub(crate) fn find_activity_by_context(&self, iri: String) -> Result<Vec<UserKey>> {
        self.find_by_iri(&self.ctx_index, iri)
    }
    pub(crate) fn find_likes(&self, iri: String) -> Result<Vec<UserKey>> {
        self.find_by_iri(&self.likes_index, iri)
    }
    pub(crate) fn find_shares(&self, iri: String) -> Result<Vec<UserKey>> {
        self.find_by_iri(&self.shares_index, iri)
    }
}
