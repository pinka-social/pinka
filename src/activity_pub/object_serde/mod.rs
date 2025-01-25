mod symbols;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use self::symbols::activitystreams_symbol_table;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub(crate) struct Header {
    version: u32,
}

impl Header {
    pub(crate) const V_1: Header = Header { version: 1 };
}

pub(crate) trait ObjectSerDe {
    fn into_bytes(self) -> Result<Vec<u8>>
    where
        Self: Into<NodeValue>,
    {
        let header = vec![];
        let payload =
            postcard::to_extend(&Header::V_1, header).context("unable to serialize RPC header")?;
        let result =
            postcard::to_extend(&self.into(), payload).context("unable to serialize payload")?;
        Ok(result)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: From<NodeValue>,
    {
        let (header, payload): (Header, _) =
            postcard::take_from_bytes(bytes).context("unable to deserialize RPC header")?;
        if header != Header::V_1 {
            tracing::error!(target: "rpc", ?header, "invalid RPC header version");
        }
        Ok(Self::from(
            postcard::from_bytes(&payload).context("unable to deserialize payload")?,
        ))
    }
}

macro_rules! impl_object_serde_new_type {
    ($typ:ident) => {
        impl crate::activity_pub::object_serde::ObjectSerDe for $typ {}
        impl serde::Serialize for $typ {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use crate::activity_pub::object_serde::ObjectSerDe;
                serializer.serialize_bytes(&self.clone().into_bytes().unwrap())
            }
        }
        impl<'de> serde::Deserialize<'de> for $typ {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct ObjectVisitor;

                impl<'de> serde::de::Visitor<'de> for ObjectVisitor {
                    type Value = $typ;
                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str(stringify!($typ))
                    }
                    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        use crate::activity_pub::object_serde::ObjectSerDe;
                        Ok($typ::from_bytes(v).map_err(|e| E::custom(e))?)
                    }
                }

                deserializer.deserialize_bytes(ObjectVisitor)
            }
        }
        impl From<$typ> for crate::activity_pub::object_serde::NodeValue {
            fn from(value: $typ) -> Self {
                value.0.into()
            }
        }
        impl From<crate::activity_pub::object_serde::NodeValue> for $typ {
            fn from(value: crate::activity_pub::object_serde::NodeValue) -> Self {
                $typ(value.into())
            }
        }
        impl From<$typ> for serde_json::Value {
            fn from(value: $typ) -> Self {
                value.0
            }
        }
        impl std::ops::Deref for $typ {
            type Target = serde_json::Value;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl std::ops::DerefMut for $typ {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Symbol {
    SymbolId(usize),
    Text(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum NodeValue {
    Null,
    Bool(bool),
    Number(f64),
    Symbol(Symbol),
    Array(Vec<NodeValue>),
    Object(Vec<(Symbol, NodeValue)>),
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
