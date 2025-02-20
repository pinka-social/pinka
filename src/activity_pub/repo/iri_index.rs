use anyhow::{Context, Result};
use fjall::{Batch, Keyspace, PartitionCreateOptions, PartitionHandle, UserKey};

use super::ObjectKey;

#[derive(Clone)]
pub(crate) struct IriIndex {
    index: PartitionHandle,
}

impl IriIndex {
    pub(crate) fn new(keyspace: Keyspace) -> Result<IriIndex> {
        let index = keyspace
            .open_partition("iri_index", PartitionCreateOptions::default())
            .context("Failed to open IRI index")?;
        Ok(IriIndex { index })
    }
    pub(crate) fn insert(&self, b: &mut Batch, iri: &str, obj_key: ObjectKey) {
        b.insert(&self.index, iri, obj_key);
    }
    pub(crate) fn find_one(&self, iri: &str) -> Result<Option<UserKey>> {
        self.index.get(iri).context("Failed to read from index")
    }
}
