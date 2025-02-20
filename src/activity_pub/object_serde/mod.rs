mod symbols;

use anyhow::{Context, Result};
use minicbor::data::Type;
use minicbor::{Decode, Encode};
use serde_json::{Number, Value};

use self::symbols::activitystreams_symbol_table;

use super::model::Object;

#[derive(Debug, Encode, Decode)]
enum Envelope {
    #[n(0)]
    V1(#[n(0)] NodeValue),
}

pub(crate) fn to_bytes(object: impl Into<Value>) -> Result<Vec<u8>> {
    let value = object.into();
    minicbor::to_vec(Envelope::V1(value.into())).context("unable to serialize object")
}
pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Object<'static>> {
    let Envelope::V1(value) = minicbor::decode(bytes).context("unable to deserialize object")?;
    Ok(Object::from(Value::from(value)))
}

impl<C> Encode<C> for Object<'_> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        ctx: &mut C,
    ) -> std::result::Result<(), minicbor::encode::Error<W::Error>> {
        let node_value: NodeValue = self.to_value().into();
        node_value.encode(e, ctx)
    }
}

impl<'b, C> Decode<'b, C> for Object<'static> {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        ctx: &mut C,
    ) -> std::result::Result<Self, minicbor::decode::Error> {
        let node_value = NodeValue::decode(d, ctx)?;
        let value = Value::from(node_value);
        Ok(value.into())
    }
}

#[derive(Debug)]
pub(crate) enum Symbol {
    SymbolId(u64),
    Text(String),
}

impl<C> Encode<C> for Symbol {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut C,
    ) -> std::result::Result<(), minicbor::encode::Error<W::Error>> {
        match self {
            Symbol::SymbolId(id) => {
                e.encode(id)?;
            }
            Symbol::Text(text) => {
                e.encode(text)?;
            }
        }
        Ok(())
    }
}

impl<'b, C> Decode<'b, C> for Symbol {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut C,
    ) -> std::result::Result<Self, minicbor::decode::Error> {
        match d.datatype()? {
            Type::U8 | Type::U16 | Type::U32 | Type::U64 => Ok(Symbol::SymbolId(d.decode()?)),
            Type::String => Ok(Symbol::Text(d.decode()?)),
            ty => Err(minicbor::decode::Error::type_mismatch(ty)),
        }
    }
}

#[derive(Debug)]
enum NodeValue {
    Null,
    Bool(bool),
    Number(f64),
    Symbol(Symbol),
    Array(Vec<NodeValue>),
    Object(Vec<(Symbol, NodeValue)>),
}

impl<C> Encode<C> for NodeValue {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut C,
    ) -> std::result::Result<(), minicbor::encode::Error<W::Error>> {
        match self {
            NodeValue::Null => {
                e.null()?;
            }
            NodeValue::Bool(x) => {
                e.bool(*x)?;
            }
            NodeValue::Number(x) => {
                e.f64(*x)?;
            }
            NodeValue::Symbol(symbol) => {
                e.encode(symbol)?;
            }
            NodeValue::Array(node_values) => {
                e.array(node_values.len() as u64)?;
                for v in node_values {
                    e.encode(v)?;
                }
            }
            NodeValue::Object(items) => {
                e.map(items.len() as u64)?;
                for (k, v) in items {
                    e.encode(k)?;
                    e.encode(v)?;
                }
            }
        }
        Ok(())
    }
}

impl<'b, C> Decode<'b, C> for NodeValue {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut C,
    ) -> std::result::Result<Self, minicbor::decode::Error> {
        match d.datatype()? {
            Type::Null => {
                d.null()?;
                Ok(NodeValue::Null)
            }
            Type::Bool => Ok(NodeValue::Bool(d.decode()?)),
            Type::F64 => Ok(NodeValue::Number(d.decode()?)),
            Type::U8 | Type::U16 | Type::U32 | Type::U64 | Type::String => {
                Ok(NodeValue::Symbol(d.decode()?))
            }
            Type::Array => {
                let mut node_values = vec![];
                for x in d.array_iter()? {
                    node_values.push(x?);
                }
                Ok(NodeValue::Array(node_values))
            }
            Type::Map => {
                let mut items = vec![];
                for x in d.map_iter()? {
                    items.push(x?);
                }
                Ok(NodeValue::Object(items))
            }
            ty => Err(minicbor::decode::Error::type_mismatch(ty)),
        }
    }
}

fn lossy_number_to_f64(number: Number) -> f64 {
    match number.as_f64() {
        Some(n) => n,
        None => panic!("number too large"),
    }
}

impl From<String> for Symbol {
    fn from(value: String) -> Self {
        let symtab = activitystreams_symbol_table();
        match symtab.get_by_left(value.as_str()) {
            Some(id) => Symbol::SymbolId(*id),
            None => Symbol::Text(value),
        }
    }
}

impl From<Symbol> for String {
    fn from(value: Symbol) -> Self {
        let symtab = activitystreams_symbol_table();
        match value {
            Symbol::SymbolId(id) => match symtab.get_by_right(&id) {
                Some(text) => text.to_string(),
                None => "__unknown__".to_string(),
            },
            Symbol::Text(text) => text,
        }
    }
}

impl NodeValue {
    /// Simple recursive conversion with depth limit
    fn from_serde_json(value: Value, stack_depth: u8, limit: u8) -> Self {
        if stack_depth == limit {
            return NodeValue::Null;
        }
        match value {
            Value::Null => NodeValue::Null,
            Value::Bool(v) => NodeValue::Bool(v),
            Value::Number(n) => NodeValue::Number(lossy_number_to_f64(n)),
            Value::String(s) => NodeValue::Symbol(s.into()),
            Value::Array(vec) => NodeValue::Array(
                vec.into_iter()
                    .map(|v| NodeValue::from_serde_json(v, stack_depth + 1, limit))
                    .collect(),
            ),
            Value::Object(map) => NodeValue::Object(
                map.into_iter()
                    .map(|(k, v)| {
                        (
                            k.into(),
                            NodeValue::from_serde_json(v, stack_depth + 1, limit),
                        )
                    })
                    .collect(),
            ),
        }
    }
}

impl From<Value> for NodeValue {
    fn from(value: Value) -> Self {
        Self::from_serde_json(value, 0, 128)
    }
}

impl From<NodeValue> for Value {
    fn from(value: NodeValue) -> Self {
        match value {
            NodeValue::Null => Value::Null,
            NodeValue::Bool(v) => Value::Bool(v),
            NodeValue::Number(n) => {
                Value::Number(Number::from_f64(n).expect("number should be f64 compatible"))
            }
            NodeValue::Symbol(s) => Value::String(s.into()),
            NodeValue::Array(vec) => Value::Array(vec.into_iter().map(Value::from).collect()),
            NodeValue::Object(map) => {
                Value::Object(map.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{NodeValue, Symbol, Value};

    #[test]
    fn convert_mastodon_note() {
        let note = json!(
            {
                "@context": [
                  "https://www.w3.org/ns/activitystreams",
                  {
                    "ostatus": "http://ostatus.org#",
                    "atomUri": "ostatus:atomUri",
                    "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
                    "conversation": "ostatus:conversation",
                    "sensitive": "as:sensitive",
                    "toot": "http://joinmastodon.org/ns#",
                    "votersCount": "toot:votersCount",
                    "Emoji": "toot:Emoji",
                    "focalPoint": {
                      "@container": "@list",
                      "@id": "toot:focalPoint"
                    }
                  }
                ],
                "id": "https://example.com/statuses/12345",
                "type": "Note",
                "summary": null,
                "inReplyTo": null,
                "published": "2024-11-04T05:12:16Z",
                "url": "https://example.com/statuses/12345",
                "attributedTo": "https://example.com/statuses/12345",
                "to": [
                  "https://www.w3.org/ns/activitystreams#Public"
                ],
                "cc": [
                  "https://example.com/statuses/12345"
                ],
                "sensitive": false,
                "atomUri": "https://example.com/statuses/12345",
                "inReplyToAtomUri": null,
                "conversation": "tag:xxxx,2024-11-04:objectId=51239730:objectType=Conversation",
                "content": "<p>hello world</p>",
                "contentMap": {
                  "zh": "<p>hello world</p>"
                },
                "attachment": [],
                "tag": [
                  {
                    "id": "https://example.com/emojis/169750",
                    "type": "Emoji",
                    "name": ":blobthinkingsmirk:",
                    "updated": "2023-03-06T05:33:44Z",
                    "icon": {
                      "type": "Image",
                      "mediaType": "image/png",
                      "url": "https://example.com/e75a855cb4d12b34.png"
                    }
                  },
                  {
                    "id": "https://example.com/emojis/52915",
                    "type": "Emoji",
                    "name": ":pleading:",
                    "updated": "2024-11-27T12:33:14Z",
                    "icon": {
                      "type": "Image",
                      "mediaType": "image/png",
                      "url": "https://objects.example.com/54d.png"
                    }
                  }
                ],
                "replies": {
                  "id": "https://example.com/users/86/replies",
                  "type": "Collection",
                  "first": {
                    "type": "CollectionPage",
                    "next": "https://example.com/next",
                    "partOf": "https://example.com/partof",
                    "items": []
                  }
                },
                "likes": {
                  "id": "https://example.com/likes",
                  "type": "Collection",
                  "totalItems": 4
                },
                "shares": {
                  "id": "https://example.com/statuses/12345/shares",
                  "type": "Collection",
                  "totalItems": 1
                }
              }
        );
        let value = NodeValue::from(note);
        let NodeValue::Object(map) = value else {
            panic!()
        };
        assert!(map.iter().all(|pair| matches!(pair.0, Symbol::SymbolId(_))));
    }

    #[test]
    fn value_from_serde_json_depth_limit() {
        let note = json!(
            {
                "@context": [
                  "https://www.w3.org/ns/activitystreams",
                  {
                    "ostatus": "http://ostatus.org#",
                    "atomUri": "ostatus:atomUri",
                    "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
                    "conversation": "ostatus:conversation",
                    "sensitive": "as:sensitive",
                    "toot": "http://joinmastodon.org/ns#",
                    "votersCount": "toot:votersCount",
                    "Emoji": "toot:Emoji",
                    "focalPoint": {
                      "@container": "@list",
                      "@id": "toot:focalPoint"
                    }
                  }
                ],
                "id": "https://example.com/statuses/12345",
                "type": "Note",
                "summary": null,
                "inReplyTo": null,
                "published": "2024-11-04T05:12:16Z",
                "url": "https://example.com/statuses/12345",
                "attributedTo": "https://example.com/statuses/12345",
                "to": [
                  "https://www.w3.org/ns/activitystreams#Public"
                ],
                "cc": [
                  "https://example.com/statuses/12345"
                ]
              }
        );
        let value = NodeValue::from_serde_json(note, 0, 1);
        let NodeValue::Object(map) = value else {
            panic!()
        };
        assert!(map.iter().all(|pair| matches!(pair.1, NodeValue::Null)));
    }

    #[test]
    fn round_trip() {
        let note = json!(
            {
                "@context": [
                  "https://www.w3.org/ns/activitystreams",
                  {
                    "ostatus": "http://ostatus.org#",
                    "atomUri": "ostatus:atomUri",
                    "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
                    "conversation": "ostatus:conversation",
                    "sensitive": "as:sensitive",
                    "toot": "http://joinmastodon.org/ns#",
                    "votersCount": "toot:votersCount",
                    "Emoji": "toot:Emoji",
                    "focalPoint": {
                      "@container": "@list",
                      "@id": "toot:focalPoint"
                    }
                  }
                ]
            }
        );
        let node: NodeValue = note.clone().into();
        assert_eq!(note, Value::from(node));
    }
}
