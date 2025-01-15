//! Storage friendly presentation of Activity Streams' core data model.

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value as JsonValue};

use super::symbols::activitystreams_symbol_table;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Symbol {
    SymbolId(usize),
    Text(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Value {
    Null,
    Bool(bool),
    Number(f64),
    Symbol(Symbol),
    Array(Vec<Value>),
    Object(Vec<(Symbol, Value)>),
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

impl Value {
    /// Simple recursive conversion with depth limit
    fn from_serde_json(value: JsonValue, stack_depth: u8, limit: u8) -> Self {
        if stack_depth == limit {
            return Value::Null;
        }
        match value {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(v) => Value::Bool(v),
            JsonValue::Number(n) => Value::Number(lossy_number_to_f64(n)),
            JsonValue::String(s) => Value::Symbol(s.into()),
            JsonValue::Array(vec) => Value::Array(
                vec.into_iter()
                    .map(|v| Value::from_serde_json(v, stack_depth + 1, limit))
                    .collect(),
            ),
            JsonValue::Object(map) => Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k.into(), Value::from_serde_json(v, stack_depth + 1, limit)))
                    .collect(),
            ),
        }
    }
}

impl From<JsonValue> for Value {
    fn from(value: JsonValue) -> Self {
        Self::from_serde_json(value, 0, 128)
    }
}

impl From<Value> for JsonValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => JsonValue::Null,
            Value::Bool(v) => JsonValue::Bool(v),
            Value::Number(n) => {
                JsonValue::Number(Number::from_f64(n).expect("number should be f64 compatible"))
            }
            Value::Symbol(s) => JsonValue::String(s.into()),
            Value::Array(vec) => JsonValue::Array(vec.into_iter().map(JsonValue::from).collect()),
            Value::Object(map) => {
                JsonValue::Object(map.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{JsonValue, Symbol, Value};

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
        let value = Value::from(note);
        let Value::Object(map) = value else { panic!() };
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
        let value = Value::from_serde_json(note, 0, 1);
        let Value::Object(map) = value else { panic!() };
        assert!(map.iter().all(|pair| matches!(pair.1, Value::Null)));
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
        let node: Value = note.clone().into();
        assert_eq!(note, JsonValue::from(node));
    }
}
