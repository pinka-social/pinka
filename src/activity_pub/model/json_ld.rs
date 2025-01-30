use serde_json::Value;

/// Validate JSON values with JSON-LD semantics
pub(crate) trait JsonLdValue {
    /// JSON-LD type is
    fn type_is(&self, ld_type: &str) -> bool;
    /// JSON-LD type is
    fn obj_type(&self) -> Option<&str>;
    /// Check required properties
    fn has_props(&self, props: &[&str]) -> bool;
    /// The value is either a string, or array of strings
    fn is_string_array(&self) -> bool;
    /// The value is either an object, or array of objects
    fn is_object_array(&self) -> bool;
    /// The value is a ActivityStreams Activity
    fn is_activity(&self) -> bool;
    /// The value is a ActivityStreams Activity supported by our inbox
    fn is_inbox_activity(&self) -> bool;
    fn object_iri(&self) -> Option<&str>;
    fn id(&self) -> Option<&str>;
    /// Update the id property
    fn set_id(&mut self, id_iri: &str);
}

impl JsonLdValue for Value {
    fn type_is(&self, ld_type: &str) -> bool {
        // TODO: in theory we should also check @type
        if let Some(Value::String(typ)) = self.get("type") {
            return typ == ld_type;
        }
        false
    }
    fn obj_type(&self) -> Option<&str> {
        self.get("type").and_then(|t| t.as_str())
    }
    fn has_props(&self, props: &[&str]) -> bool {
        if let Some(map) = self.as_object() {
            return props.iter().all(|&key| map.contains_key(key));
        }
        false
    }
    fn is_string_array(&self) -> bool {
        if self.is_string() {
            return true;
        }
        if let Some(array) = self.as_array() {
            return array.iter().all(|v| v.is_string());
        }
        false
    }
    fn is_object_array(&self) -> bool {
        if self.is_object() {
            return true;
        }
        if let Some(array) = self.as_array() {
            return array.iter().all(|v| v.is_object());
        }
        false
    }
    fn is_activity(&self) -> bool {
        if let Some(Value::String(typ)) = self.get("type") {
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
    fn is_inbox_activity(&self) -> bool {
        if let Some(Value::String(typ)) = self.get("type") {
            if [
                "Announce", "Create", "Delete", "Dislike", "Follow", "Like", "Update", "Undo",
            ]
            .contains(&typ.as_str())
            {
                return true;
            }
        }
        false
    }
    fn object_iri(&self) -> Option<&str> {
        if let Some(v) = self.get("object") {
            if v.is_string() {
                return v.as_str();
            }
            if v.is_string_array() {
                let id = v.as_array().unwrap().get(0).unwrap();
                return id.as_str();
            }
            if v.is_object() {
                return v.id();
            }
        }
        None
    }
    fn id(&self) -> Option<&str> {
        self.get("id").and_then(Value::as_str)
    }
    fn set_id(&mut self, id_iri: &str) {
        let map = self.as_object_mut().unwrap();
        map.insert("id".to_string(), Value::String(id_iri.to_string()));
    }
}
