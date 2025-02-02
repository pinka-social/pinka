use std::fmt::Display;
use std::str::{self, FromStr};

use fjall::{Slice, UserKey};
use minicbor::{Decode, Encode};
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ObjectKey(Uuid);

impl ObjectKey {
    pub(crate) fn new() -> ObjectKey {
        ObjectKey(uuid::Uuid::now_v7())
    }
}

impl From<ObjectKey> for UserKey {
    fn from(value: ObjectKey) -> Self {
        UserKey::new(value.0.as_bytes())
    }
}

impl AsRef<[u8]> for ObjectKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_simple().fmt(f)
    }
}

impl FromStr for ObjectKey {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ObjectKey(Uuid::try_parse(s)?))
    }
}

impl TryFrom<&[u8]> for ObjectKey {
    type Error = std::array::TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(ObjectKey(Uuid::from_bytes(value.try_into()?)))
    }
}

impl<C> Encode<C> for ObjectKey {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.bytes(self.0.as_bytes())?;
        Ok(())
    }
}

impl<'b, C> Decode<'b, C> for ObjectKey {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut C,
    ) -> Result<Self, minicbor::decode::Error> {
        let bytes = d.bytes()?;
        let uuid = bytes
            .try_into()
            .map_err(minicbor::decode::Error::custom)
            .map(Uuid::from_bytes)
            .map_err(minicbor::decode::Error::custom)?;
        Ok(ObjectKey(uuid))
    }
}

#[derive(Clone)]
pub(super) struct IdObjIndexKey(Slice);

impl IdObjIndexKey {
    pub(super) fn new(id: &str, sort_key: ObjectKey) -> IdObjIndexKey {
        let mut key = vec![];
        key.extend_from_slice(id.as_bytes());
        key.push(0);
        key.extend_from_slice(sort_key.as_ref());
        IdObjIndexKey(key.into())
    }
    pub(super) fn id(&self) -> &str {
        let id_bytes = self
            .0
            .split(|&b| b == 0)
            .next()
            .expect("IdObjIndexKey should be NUL delimited");
        str::from_utf8(id_bytes).expect("id should be valid UTF-8 string")
    }
    pub(super) fn obj_key(&self) -> UserKey {
        self.0
            .split(|&b| b == 0)
            .nth(1)
            .expect("IdObjIndexKey should be NUL delimited")
            .into()
    }
}

impl From<IdObjIndexKey> for UserKey {
    fn from(value: IdObjIndexKey) -> Self {
        value.0
    }
}

impl From<&[u8]> for IdObjIndexKey {
    fn from(value: &[u8]) -> Self {
        IdObjIndexKey(Slice::new(value))
    }
}
