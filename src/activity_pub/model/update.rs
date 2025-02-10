use anyhow::{bail, Result};
use jiff::Timestamp;
use serde_json::{json, Value};

use super::Object;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Update<'a>(Object<'a>);

impl TryFrom<Object<'_>> for Update<'static> {
    type Error = anyhow::Error;

    fn try_from(object: Object<'_>) -> Result<Self> {
        if object.type_is("Update") {
            if object.id().is_none() {
                bail!("activity must have id and type property");
            }
            // TODO validate all required properties
            return Ok(Update(object.into_owned()));
        }

        if !object.has_props(&["type"]) {
            bail!("Object must have type property");
        }
        // TODO: copy @context to activity?
        let object = object.strip_context();

        let mut update = json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Update",
            "published": Timestamp::now().to_string(),
        });

        let map = update.as_object_mut().unwrap();
        for prop in ["actor", "to", "bto", "cc", "bcc", "audience"] {
            if let Some(v) = object.get_value(prop) {
                map.insert(prop.to_string(), v);
            }
        }
        map.insert("object".to_string(), object.into());

        Ok(Update(Object::from(update)))
    }
}

impl Update<'_> {
    pub(crate) fn ensure_id(self, iri: impl Into<String>) -> Self {
        Update(self.0.ensure_id(iri))
    }
    pub(crate) fn with_actor(self, actor_iri: impl Into<String>) -> Self {
        let value = Value::String(actor_iri.into());
        let obj =
            self.0
                .augment("actor", value.clone())
                .augment_node("object", "attributedTo", value);
        Update(obj)
    }
}

impl<'a> From<Update<'a>> for Object<'a> {
    fn from(value: Update<'a>) -> Self {
        value.0
    }
}

impl From<Update<'_>> for Value {
    fn from(value: Update<'_>) -> Self {
        value.0.to_value()
    }
}
