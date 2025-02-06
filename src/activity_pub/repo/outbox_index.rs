use anyhow::{Context, Result};
use fjall::{Batch, Keyspace, PartitionCreateOptions};

use crate::activity_pub::model::Object;

use super::iri_index::IriIndex;
use super::xindex::IdObjIndex;
use super::{IdObjIndexKey, ObjectKey, ObjectRepo};

#[derive(Clone)]
pub(crate) struct OutboxIndex {
    object_repo: ObjectRepo,
    iri_index: IriIndex,
    outbox_index: IdObjIndex,
}

impl OutboxIndex {
    pub(crate) fn new(keyspace: Keyspace) -> Result<OutboxIndex> {
        let object_repo = ObjectRepo::new(keyspace.clone())?;
        let iri_index = IriIndex::new(keyspace.clone())?;
        let outbox_index = IdObjIndex::new(
            keyspace.open_partition("outbox_index", PartitionCreateOptions::default())?,
        );
        Ok(OutboxIndex {
            object_repo,
            iri_index,
            outbox_index,
        })
    }
    pub(crate) fn insert_create(
        &self,
        b: &mut Batch,
        uid: String,
        act_key: ObjectKey,
        obj_key: ObjectKey,
        act: Object,
    ) -> Result<()> {
        let obj = act
            .get_node_object("object")
            .context("Create activity should have inner object")?;
        let obj_iri = act
            .get_node_iri("object")
            .context("obj should have an IRI")?
            .to_string();
        self.object_repo.insert(b, obj_key, obj)?;
        self.object_repo.insert(b, act_key, act)?;
        self.iri_index.insert(b, &obj_iri, obj_key)?;
        self.outbox_index
            .insert(b, IdObjIndexKey::new(&uid, act_key))?;
        Ok(())
    }

    pub(crate) fn insert_update(
        &self,
        b: &mut Batch,
        uid: String,
        act_key: ObjectKey,
        act: Object,
    ) -> Result<()> {
        let obj = act
            .get_node_object("object")
            .context("Update activity should have inner object")?;
        let obj_iri = act
            .get_node_iri("object")
            .context("obj should have an IRI")?
            .to_string();
        let obj_key = ObjectKey::try_from(
            self.iri_index
                .find_one(&obj_iri)?
                .context("IriIndex should have object iri")?
                .as_ref(),
        )?;
        self.object_repo.insert(b, obj_key, obj)?;
        self.object_repo.insert(b, act_key, act)?;
        self.outbox_index
            .insert(b, IdObjIndexKey::new(&uid, act_key))?;
        Ok(())
    }
    pub(crate) fn count(&self, uid: &str) -> u64 {
        // FIXME optimize scanning
        self.outbox_index.count(uid)
    }
    /// Based on GraphQL Cursor Connections Specification
    ///
    /// Ref: <https://relay.dev/graphql/connections.htm#sec-Pagination-algorithm>
    pub(crate) fn find_all(
        &self,
        uid: &str,
        before: Option<String>,
        after: Option<String>,
        first: Option<u64>,
        last: Option<u64>,
    ) -> Result<Vec<(ObjectKey, Object)>> {
        let keys = self
            .outbox_index
            .find_all(uid, before, after, first, last)?;
        let mut result = vec![];
        for key in keys {
            if let Some(obj) = self.object_repo.find_one(key.as_ref())? {
                result.push((ObjectKey::try_from(key.as_ref())?, obj));
            }
        }
        Ok(result)
    }
}
