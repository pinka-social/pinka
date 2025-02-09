use anyhow::Result;
use fjall::{Batch, Keyspace, PartitionCreateOptions, PartitionHandle};

#[derive(Clone)]
pub(crate) struct CryptoRepo {
    key_pairs: PartitionHandle,
}

impl CryptoRepo {
    pub(crate) fn new(keyspace: Keyspace) -> Result<CryptoRepo> {
        let key_pairs = keyspace.open_partition("key_pairs", PartitionCreateOptions::default())?;
        Ok(CryptoRepo { key_pairs })
    }
    pub(crate) fn insert(&self, b: &mut Batch, uid: &str, key_pair: &[u8]) -> Result<()> {
        b.insert(&self.key_pairs, uid, key_pair);
        Ok(())
    }
    pub(crate) fn find_one(&self, uid: &str) -> Result<Option<Vec<u8>>> {
        if let Some(bytes) = self.key_pairs.get(uid)? {
            return Ok(Some(bytes.to_vec()));
        }
        Ok(None)
    }
}
