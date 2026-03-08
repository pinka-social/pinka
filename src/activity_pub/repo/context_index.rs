use anyhow::{Context, Result};
use fjall::{Database, OwnedWriteBatch};

use crate::activity_pub::model::Object;

use super::xindex::IdObjIndex;
use super::{IdObjIndexKey, ObjectKey, ObjectRepo};

#[derive(Clone)]
pub(crate) struct ContextIndex {
    ctx_index: IdObjIndex,
    likes_index: IdObjIndex,
    shares_index: IdObjIndex,
    object_repo: ObjectRepo,
}

impl ContextIndex {
    pub(crate) fn new(database: Database) -> Result<ContextIndex> {
        fn open_indexes(
            database: Database,
        ) -> Result<(IdObjIndex, IdObjIndex, IdObjIndex, ObjectRepo)> {
            let ctx_index = IdObjIndex::new(database.keyspace("ctx_index", || Default::default())?);
            let likes_index =
                IdObjIndex::new(database.keyspace("likes_index", || Default::default())?);
            let shares_index =
                IdObjIndex::new(database.keyspace("shares_index", || Default::default())?);
            let object_repo = ObjectRepo::new(database.clone())?;
            Ok((ctx_index, likes_index, shares_index, object_repo))
        }
        let (ctx_index, likes_index, shares_index, object_repo) =
            open_indexes(database).context("Failed to open indexes")?;
        Ok(ContextIndex {
            ctx_index,
            likes_index,
            shares_index,
            object_repo,
        })
    }
    pub(crate) fn insert(&self, b: &mut OwnedWriteBatch, iri: &str, obj_key: ObjectKey) {
        self.ctx_index.insert(b, IdObjIndexKey::new(iri, obj_key));
    }
    pub(crate) fn insert_likes(&self, b: &mut OwnedWriteBatch, iri: &str, obj_key: ObjectKey) {
        self.likes_index.insert(b, IdObjIndexKey::new(iri, obj_key));
    }
    pub(crate) fn remove_likes(&self, b: &mut OwnedWriteBatch, iri: &str, obj_key: ObjectKey) {
        self.likes_index.remove(b, IdObjIndexKey::new(iri, obj_key));
    }
    pub(crate) fn insert_shares(&self, b: &mut OwnedWriteBatch, iri: &str, obj_key: ObjectKey) {
        self.shares_index
            .insert(b, IdObjIndexKey::new(iri, obj_key));
    }
    pub(crate) fn count_replies(&self, iri: &str) -> u64 {
        self.ctx_index.count(iri)
    }
    pub(crate) fn count_likes(&self, iri: &str) -> u64 {
        self.likes_index.count(iri)
    }
    pub(crate) fn count_shares(&self, iri: &str) -> u64 {
        self.shares_index.count(iri)
    }
    pub(crate) fn find_replies(
        &self,
        iri: &str,
        before: Option<String>,
        after: Option<String>,
        first: Option<u64>,
        last: Option<u64>,
    ) -> Result<Vec<(ObjectKey, Object<'_>)>> {
        let keys = self.ctx_index.find_all(iri, before, after, first, last)?;
        let mut result = vec![];
        for key in keys {
            if let Some(obj) = self.object_repo.find_one(key.as_ref())? {
                result.push((ObjectKey::try_from(key.as_ref())?, obj));
            }
        }
        Ok(result)
    }
}
