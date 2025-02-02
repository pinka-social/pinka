//! Storage friendly presentation of Activity Streams' core data model.

use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Object(pub(super) Value);

impl From<Value> for Object {
    fn from(value: Value) -> Self {
        if !value.is_object() {
            // XXX: it is an error to create an Object from anything but a JSON
            // object. It should be validated by upper layers. In case some slip
            // through, we will just replace them with an empty object.
            Object(Value::Object(Map::new()))
        } else {
            Object(value)
        }
    }
}

impl AsRef<Value> for Object {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

impl AsMut<Value> for Object {
    fn as_mut(&mut self) -> &mut Value {
        &mut self.0
    }
}

impl From<Object> for Value {
    fn from(value: Object) -> Self {
        value.0
    }
}

impl Object {
    pub(crate) fn ensure_id(mut self, iri: String) -> Object {
        let obj_map = self.0.as_object_mut().unwrap();
        if !obj_map.contains_key("id") {
            obj_map.insert("id".to_string(), Value::String(iri));
        }
        self
    }
    pub(crate) fn augment_with(&mut self, property: &str, value: Value) -> &mut Self {
        let obj_map = self.0.as_object_mut().unwrap();
        if !obj_map.contains_key(property) {
            obj_map.insert(property.to_string(), value);
        }
        self
    }
}

pub(crate) trait BaseObject {
    fn is_activity(&self) -> bool;
    fn id(&self) -> Option<String>;
}

impl<T> BaseObject for T
where
    T: AsRef<Value>,
{
    fn is_activity(&self) -> bool {
        if let Some(Value::String(typ)) = self.as_ref().get("type") {
            if [
                "Accept",
                "Add",
                "Announce",
                "Arrive",
                "Block",
                "Create",
                "Delete",
                "Dislike",
                "Flag",
                "Follow",
                "Ignore",
                "Invite",
                "Join",
                "Leave",
                "Like",
                "Listen",
                "Move",
                "Offer",
                "Question",
                "Reject",
                "Read",
                "Remove",
                "TentativeReject",
                "TentativeAccept",
                "Travel",
                "Undo",
                "Update",
                "View",
            ]
            .contains(&typ.as_str())
            {
                return true;
            }
        }
        false
    }
    fn id(&self) -> Option<String> {
        self.as_ref()
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }
}
