//! Storage friendly presentation of Activity Streams' core data model.

use std::borrow::Cow;
use std::fmt::Display;

use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Object<'a>(Cow<'a, Value>);

impl Object<'_> {
    pub(crate) fn id(&self) -> Option<&str> {
        self.get_str("id").or_else(|| self.get_str("@id"))
    }
    pub(crate) fn is_activity(&self) -> bool {
        ACTIVITY_TYPES.iter().any(|ty| self.type_is(ty))
    }
    pub(crate) fn is_inbox_activity(&self) -> bool {
        INBOX_ACTIVITY_TYPES.iter().any(|ty| self.type_is(ty))
    }
    pub(crate) fn type_is(&self, ty: &str) -> bool {
        for prop in ["type", "@type"] {
            if let Some(Value::String(object_type)) = self.0.get(prop) {
                return object_type == ty;
            }
            if let Some(Value::Array(type_array)) = self.0.get(prop) {
                return type_array.iter().any(|v| {
                    if let Some(s) = v.as_str() {
                        s == ty
                    } else {
                        false
                    }
                });
            }
        }
        false
    }
    pub(crate) fn get_first_type(&self) -> Option<String> {
        for prop in ["type", "@type"] {
            if let Some(Value::String(object_type)) = self.0.get(prop) {
                return Some(object_type.to_owned());
            }
            if let Some(Value::Array(type_array)) = self.0.get(prop) {
                return type_array
                    .iter()
                    .find(|v| v.is_string())
                    .and_then(|v| v.as_str().map(|s| s.to_owned()));
            }
        }
        None
    }
    pub(crate) fn has_props(&self, props: &[&str]) -> bool {
        if let Some(map) = self.0.as_object() {
            return props.iter().all(|&key| map.contains_key(key));
        }
        false
    }
    pub(crate) fn get_str(&self, prop: &str) -> Option<&str> {
        self.0.get(prop).and_then(Value::as_str)
    }
    pub(crate) fn get_value(&self, prop: &str) -> Option<Value> {
        self.0.get(prop).cloned()
    }
    pub(crate) fn get_str_array(&self, prop: &str) -> Option<Vec<&str>> {
        if let Some(s) = self.get_str(prop) {
            return Some(vec![s]);
        }
        if let Some(Value::Array(array)) = self.0.get(prop) {
            if array.iter().all(|v| v.is_string()) {
                return Some(array.iter().map(|v| v.as_str().unwrap()).collect());
            }
        }
        None
    }
    pub(crate) fn get_node_object(&self, prop: &str) -> Option<Object> {
        if let Some(v) = self.0.get(prop) {
            if v.is_object() {
                return Some(v.into());
            }
        }
        None
    }
    pub(crate) fn get_node_iri(&self, prop: &str) -> Option<&str> {
        if let Some(v) = self.0.get(prop) {
            if v.is_string() {
                return v.as_str();
            }
            if v.is_object() {
                return v.get("id").and_then(Value::as_str);
            }
            // As shown in
            // https://www.w3.org/TR/activitystreams-vocabulary/#properties, a
            // node reference might be an array. We just use the first IRI.
            if v.is_array() {
                let id = v.as_array().unwrap().iter().find(|v| v.is_string());
                return id.and_then(Value::as_str);
            }
        }
        None
    }
    pub(crate) fn get_endpoint(&self, prop: &str) -> Option<&str> {
        if let Some(value) = self.0.get("endpoints") {
            if let Some(v) = value.get(prop) {
                return v.as_str();
            }
        }
        None
    }
    pub(crate) fn into_owned(self) -> Object<'static> {
        Object(Cow::Owned(self.0.into_owned()))
    }
    pub(crate) fn to_value(&self) -> Value {
        self.0.clone().into_owned()
    }
    pub(crate) fn strip_context(self) -> Object<'static> {
        let mut obj = self.0.into_owned();
        let obj_map = obj.as_object_mut().unwrap();
        obj_map.remove("@context");
        Object(Cow::Owned(obj))
    }
    pub(crate) fn ensure_id(self, iri: impl Into<String>) -> Object<'static> {
        let mut obj = self.0.into_owned();
        let obj_map = obj.as_object_mut().unwrap();
        if !obj_map.contains_key("id") {
            obj_map.insert("id".to_string(), Value::String(iri.into()));
        }
        Object(Cow::Owned(obj))
    }
    pub(crate) fn replace(self, property: &str, value: Value) -> Object<'static> {
        let mut obj = self.0.into_owned();
        let obj_map = obj.as_object_mut().unwrap();
        obj_map.insert(property.to_string(), value);
        Object(Cow::Owned(obj))
    }
    pub(crate) fn augment(self, property: &str, value: Value) -> Object<'static> {
        let mut obj = self.0.into_owned();
        let obj_map = obj.as_object_mut().unwrap();
        if !obj_map.contains_key(property) {
            obj_map.insert(property.to_string(), value);
        }
        Object(Cow::Owned(obj))
    }
    pub(crate) fn augment_if(self, cond: bool, property: &str, value: Value) -> Object<'static> {
        if cond {
            return self.augment(property, value);
        }
        self.into_owned()
    }
    pub(crate) fn augment_node(self, node: &str, property: &str, value: Value) -> Object<'static> {
        let mut obj = self.0.into_owned();
        if let Some(Value::Object(map)) = obj.get_mut(node) {
            if !map.contains_key(property) {
                map.insert(property.to_string(), value);
            }
        }
        Object(Cow::Owned(obj))
    }
    pub(crate) fn augment_with(self, map: Map<String, Value>) -> Object<'static> {
        let mut obj = self.0.into_owned();
        let obj_map = obj.as_object_mut().unwrap();
        obj_map.extend(map);
        Object(Cow::Owned(obj))
    }
}

impl From<Value> for Object<'static> {
    fn from(value: Value) -> Self {
        if !value.is_object() {
            // XXX: it is an error to create an Object from anything but a JSON
            // object. It should be validated by upper layers. In case some slip
            // through, we will just replace them with an empty object.
            Object(Cow::Owned(Value::Object(Map::new())))
        } else {
            Object(Cow::Owned(value))
        }
    }
}

impl<'a> From<&'a Value> for Object<'a> {
    fn from(value: &'a Value) -> Self {
        if !value.is_object() {
            // XXX: it is an error to create an Object from anything but a JSON
            // object. It should be validated by upper layers. In case some slip
            // through, we will just replace them with an empty object.
            Object(Cow::Owned(Value::Object(Map::new())))
        } else {
            Object(Cow::Borrowed(value))
        }
    }
}

impl From<Object<'_>> for Value {
    fn from(value: Object) -> Self {
        value.0.into_owned()
    }
}

impl AsRef<Value> for Object<'_> {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

impl Display for Object<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

const ACTIVITY_TYPES: [&str; 28] = [
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
];

const INBOX_ACTIVITY_TYPES: [&str; 8] = [
    "Announce", "Create", "Delete", "Dislike", "Follow", "Like", "Update", "Undo",
];
