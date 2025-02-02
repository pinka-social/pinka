use std::ops::Bound;
use std::str::FromStr;

use anyhow::Result;
use fjall::{Batch, PartitionHandle, UserKey};

use super::{IdObjIndexKey, ObjectKey};

#[derive(Clone)]
pub(super) struct IdObjIndex {
    index: PartitionHandle,
}

impl IdObjIndex {
    pub(super) fn new(index: PartitionHandle) -> IdObjIndex {
        IdObjIndex { index }
    }
    pub(super) fn insert(&self, b: &mut Batch, id_obj_key: IdObjIndexKey) -> Result<()> {
        b.insert(&self.index, id_obj_key, []);
        Ok(())
    }
    pub(super) fn remove(&self, b: &mut Batch, id_obj_key: IdObjIndexKey) -> Result<()> {
        b.remove(&self.index, id_obj_key);
        Ok(())
    }
    pub(super) fn count(&self, id: &str) -> u64 {
        // FIXME optimize scanning
        self.index.prefix(id).count() as u64
    }
    /// Based on GraphQL Cursor Connections Specification
    ///
    /// Ref: <https://relay.dev/graphql/connections.htm#sec-Pagination-algorithm>
    pub(super) fn find_all(
        &self,
        id: &str,
        before: Option<String>,
        after: Option<String>,
        first: Option<u64>,
        last: Option<u64>,
    ) -> Result<Vec<UserKey>> {
        let mut keys = vec![];
        let start = match after {
            Some(after) => {
                let obj_key = ObjectKey::from_str(&after)?;
                let start_key: UserKey = IdObjIndexKey::new(id, obj_key).into();
                Bound::Excluded(start_key)
            }
            None => Bound::Unbounded,
        };
        let end = match before {
            Some(before) => {
                let obj_key = ObjectKey::from_str(&before)?;
                let end_key: UserKey = IdObjIndexKey::new(id, obj_key).into();
                Bound::Excluded(end_key)
            }
            None => Bound::Unbounded,
        };
        let iter = self.index.range((start, end));
        match (first, last) {
            (Some(first), None) => {
                for pair in iter.take(first as usize) {
                    let (idx_key, _) = pair?;
                    keys.push(IdObjIndexKey::from(idx_key.as_ref()).obj_key());
                }
            }
            (None, Some(last)) => {
                for pair in iter.rev().take(last as usize) {
                    let (idx_key, _) = pair?;
                    keys.push(IdObjIndexKey::from(idx_key.as_ref()).obj_key());
                }
                keys.reverse();
            }
            (Some(first), Some(last)) => {
                //  Including a value for both first and last is strongly
                //  discouraged, as it is likely to lead to confusing queries and
                //  results.
                for pair in iter.take(first as usize) {
                    let (idx_key, _) = pair?;
                    keys.push(IdObjIndexKey::from(idx_key.as_ref()).obj_key());
                }
                keys = keys.into_iter().rev().take(last as usize).collect();
            }
            (None, None) => {
                for pair in iter {
                    let (idx_key, _) = pair?;
                    keys.push(IdObjIndexKey::from(idx_key.as_ref()).obj_key());
                }
            }
        }
        Ok(keys)
    }
}
