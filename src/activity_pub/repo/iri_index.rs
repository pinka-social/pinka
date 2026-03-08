use anyhow::{Context, Result};
use fjall::{Database, Keyspace, OwnedWriteBatch, UserKey};

use super::ObjectKey;

#[derive(Clone)]
pub(crate) struct IriIndex {
    index: Keyspace,
}

impl IriIndex {
    pub(crate) fn new(database: Database) -> Result<IriIndex> {
        let index = database
            .keyspace("iri_index", || Default::default())
            .context("Failed to open IRI index")?;
        Ok(IriIndex { index })
    }
    pub(crate) fn insert(&self, b: &mut OwnedWriteBatch, iri: &str, obj_key: ObjectKey) {
        b.insert(&self.index, iri, obj_key);
    }
    pub(crate) fn find_one(&self, iri: &str) -> Result<Option<UserKey>> {
        self.index.get(iri).context("Failed to read from index")
    }
}
