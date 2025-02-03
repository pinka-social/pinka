use anyhow::{bail, Result};
use jiff::Timestamp;
use serde_json::{json, Value};

use super::Object;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Create<'a>(Object<'a>);

impl TryFrom<Object<'_>> for Create<'static> {
    type Error = anyhow::Error;

    fn try_from(object: Object<'_>) -> Result<Self> {
        if object.type_is("Create") {
            if object.id().is_none() {
                bail!("Create activity must have id and type property");
            }
            // TODO validate all required properties
            return Ok(Create(object.into_owned()));
        }

        if !object.has_props(&["type"]) {
            bail!("Object must have type property");
        }
        // TODO: copy @context to activity?
        let object = object.strip_context();

        let mut create = json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Create",
            "published": Timestamp::now().to_string(),
        });

        let map = create.as_object_mut().unwrap();
        for prop in ["to", "bto", "cc", "bcc", "audience", "published"] {
            if let Some(v) = object.get_value(prop) {
                map.insert(prop.to_string(), v);
            }
        }
        map.insert("object".to_string(), object.into());

        Ok(Create(Object::from(create)))
    }
}

impl Create<'_> {
    pub(crate) fn ensure_id(self, iri: impl Into<String>) -> Self {
        Create(self.0.ensure_id(iri))
    }
    pub(crate) fn with_actor(self, actor_iri: impl Into<String>) -> Self {
        let value = Value::String(actor_iri.into());
        let obj =
            self.0
                .augment("actor", value.clone())
                .augment_node("object", "attributedTo", value);
        Create(obj)
    }
    pub(crate) fn get_object(&self) -> Option<Object> {
        self.0.get_node_object("object")
    }
}

impl From<Create<'_>> for Value {
    fn from(value: Create<'_>) -> Self {
        value.0.to_value()
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;

    use crate::activity_pub::model::Object;

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
        let result = Create(Object::from(json!({
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
        })));

        let object = Object::from(note).ensure_id("https://example.com/~mallory/note/72");
        let activity = Create::try_from(object)?
            .ensure_id("https://example.net/~mallory/87374")
            .with_actor("https://example.net/~mallory");

        assert_eq!(activity, result);
        Ok(())
    }
}
