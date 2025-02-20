use anyhow::{Context, Result};
use fjall::{Batch, Keyspace};

use super::xindex::IdObjIndex;
use super::{IdObjIndexKey, ObjectKey};

#[derive(Clone)]
pub(crate) struct ContextIndex {
    ctx_index: IdObjIndex,
    likes_index: IdObjIndex,
    shares_index: IdObjIndex,
}

impl ContextIndex {
    pub(crate) fn new(keyspace: Keyspace) -> Result<ContextIndex> {
        fn open_indexes(keyspace: Keyspace) -> Result<(IdObjIndex, IdObjIndex, IdObjIndex)> {
            let ctx_index =
                IdObjIndex::new(keyspace.open_partition("ctx_index", Default::default())?);
            let likes_index =
                IdObjIndex::new(keyspace.open_partition("likes_index", Default::default())?);
            let shares_index =
                IdObjIndex::new(keyspace.open_partition("shares_index", Default::default())?);
            Ok((ctx_index, likes_index, shares_index))
        }
        let (ctx_index, likes_index, shares_index) =
            open_indexes(keyspace).context("Failed to open indexes")?;
        Ok(ContextIndex {
            ctx_index,
            likes_index,
            shares_index,
        })
    }
    pub(crate) fn insert(&self, b: &mut Batch, iri: &str, obj_key: ObjectKey) {
        self.ctx_index.insert(b, IdObjIndexKey::new(iri, obj_key));
    }
    pub(crate) fn insert_likes(&self, b: &mut Batch, iri: &str, obj_key: ObjectKey) {
        self.likes_index.insert(b, IdObjIndexKey::new(iri, obj_key));
    }
    pub(crate) fn remove_likes(&self, b: &mut Batch, iri: &str, obj_key: ObjectKey) {
        self.likes_index.remove(b, IdObjIndexKey::new(iri, obj_key));
    }
    pub(crate) fn insert_shares(&self, b: &mut Batch, iri: &str, obj_key: ObjectKey) {
        self.shares_index
            .insert(b, IdObjIndexKey::new(iri, obj_key));
    }
    pub(crate) fn count_likes(&self, iri: &str) -> u64 {
        self.likes_index.count(iri)
    }
    pub(crate) fn count_shares(&self, iri: &str) -> u64 {
        self.shares_index.count(iri)
    }
}
