use anyhow::{Context, Result};
use fjall::{Batch, Keyspace, PartitionCreateOptions, PartitionHandle};
use minicbor::{Decode, Encode};
use secrecy::{ExposeSecret, SecretSlice};

#[derive(Clone)]
pub(crate) struct CryptoRepo {
    key_pairs: PartitionHandle,
}

impl CryptoRepo {
    pub(crate) fn new(keyspace: Keyspace) -> Result<CryptoRepo> {
        let key_pairs = keyspace
            .open_partition("key_pairs", PartitionCreateOptions::default())
            .context("Failed top open crypto repo")?;
        Ok(CryptoRepo { key_pairs })
    }
    pub(crate) fn insert(&self, b: &mut Batch, uid: &str, key_pair: &KeyMaterial) {
        b.insert(&self.key_pairs, uid, key_pair.expose_secret());
    }
    pub(crate) fn find_one(&self, uid: &str) -> Result<Option<KeyMaterial>> {
        if let Some(bytes) = self.key_pairs.get(uid)? {
            return Ok(Some(bytes.to_vec().into()));
        }
        Ok(None)
    }
}

#[derive(Debug)]
pub(crate) struct KeyMaterial(SecretSlice<u8>);

impl<C> Encode<C> for KeyMaterial {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.bytes(self.0.expose_secret())?;
        Ok(())
    }
}

impl<'b, C> Decode<'b, C> for KeyMaterial {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut C,
    ) -> Result<Self, minicbor::decode::Error> {
        let vec = d.bytes()?.to_vec();
        Ok(KeyMaterial::from(vec))
    }
}

impl From<Vec<u8>> for KeyMaterial {
    fn from(value: Vec<u8>) -> Self {
        let inner = SecretSlice::from(value);
        KeyMaterial(inner)
    }
}

impl ExposeSecret<[u8]> for KeyMaterial {
    fn expose_secret(&self) -> &[u8] {
        self.0.expose_secret()
    }
}
