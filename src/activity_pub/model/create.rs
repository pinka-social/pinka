use anyhow::{Error, Result, bail};
use jiff::Timestamp;
use serde_json::{Value, json};

use super::Object;
use super::json_ld::JsonLdValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Create(Value);

impl<'a> TryFrom<Object> for Create {
    type Error = Error;

    fn try_from(mut object: Object) -> Result<Self> {
        let value = object.as_mut();

        if value.type_is("Create") {
            if !value.has_props(&["id", "type"]) {
                bail!("Create activity must have id and type property");
            }
            // TODO validate all required properties
            return Ok(Create(object.into()));
        }

        if !value.has_props(&["type"]) {
            bail!("Object must have type property");
        }
        // TODO: copy @context to activity?
        value.as_object_mut().unwrap().remove("@context");

        let mut create = json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Create",
            "published": Timestamp::now().to_string(),
        });

        let map = create.as_object_mut().unwrap();
        for prop in ["to", "bto", "cc", "bcc", "published"] {
            if let Some(v) = value.get(prop) {
                if v.is_string_array() {
                    map.insert(prop.to_string(), v.clone());
                }
            }
        }
        for prop in ["audience"] {
            if let Some(v) = value.get(prop) {
                if v.is_object_array() {
                    map.insert(prop.to_string(), v.clone());
                }
            }
        }
        map.insert("object".to_string(), object.into());

        Ok(Create(create))
    }
}

impl AsRef<Value> for Create {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

impl AsMut<Value> for Create {
    fn as_mut(&mut self) -> &mut Value {
        &mut self.0
    }
}

impl From<Create> for Value {
    fn from(value: Create) -> Self {
        value.0
    }
}

impl Create {
    pub(crate) fn with_actor(mut self, actor_iri: &str) -> Self {
        let map = self.0.as_object_mut().unwrap();
        map.insert("actor".to_string(), Value::String(actor_iri.to_string()));
        let obj_map = map.get_mut("object").unwrap().as_object_mut().unwrap();
        obj_map.insert(
            "attributedTo".to_string(),
            Value::String(actor_iri.to_string()),
        );
        self
    }
    pub(crate) fn with_published(mut self, ts: Timestamp) -> Self {
        let map = self.0.as_object_mut().unwrap();
        map.insert("published".to_string(), Value::String(ts.to_string()));
        let obj_map = map.get_mut("object").unwrap().as_object_mut().unwrap();
        obj_map.insert("published".to_string(), Value::String(ts.to_string()));
        self
    }
    pub(crate) fn get_object(&self) -> Object {
        let map = self.0.as_object().unwrap();
        let obj = map.get("object").unwrap();
        Object(obj.clone())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;

    use crate::activity_pub::model::{JsonLdValue, Object};

    use super::Create;

    #[test]
    fn object_to_create_activity() -> Result<()> {
        let note = json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Note",
            "content": "This is a note",
            "published": "2015-02-10T15:04:55Z",
            "to": ["https://example.org/~john/"],
            "cc": ["https://example.com/~erik/followers",
                "https://www.w3.org/ns/activitystreams#Public"]
        });
        let result = json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Create",
            "id": "https://example.net/~mallory/87374",
            "actor": "https://example.net/~mallory",
            "object": {
                "id": "https://example.com/~mallory/note/72",
                "type": "Note",
                "attributedTo": "https://example.net/~mallory",
                "content": "This is a note",
                "published": "2015-02-10T15:04:55Z",
                "to": ["https://example.org/~john/"],
                "cc": ["https://example.com/~erik/followers",
                        "https://www.w3.org/ns/activitystreams#Public"]
            },
            "published": "2015-02-10T15:04:55Z",
            "to": ["https://example.org/~john/"],
            "cc": ["https://example.com/~erik/followers",
                    "https://www.w3.org/ns/activitystreams#Public"]
        });

        let mut object = Object::try_from(note)?;
        object
            .as_mut()
            .set_id("https://example.com/~mallory/note/72");
        let mut activity = Create::try_from(object)?.with_actor("https://example.net/~mallory");
        activity
            .as_mut()
            .set_id("https://example.net/~mallory/87374");

        assert_eq!(result, activity.0);
        Ok(())
    }
}
