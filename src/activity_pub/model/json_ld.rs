use serde_json::Value;

/// Validate JSON values with JSON-LD semantics
pub(crate) trait JsonLdValue {
    /// JSON-LD type is
    fn type_is(&self, ld_type: &str) -> bool;
    /// Check required properties
    fn has_props(&self, props: &[&str]) -> bool;
    /// The value is either a string, or array of strings
    fn is_string_array(&self) -> bool;
    /// The value is either an object, or array of objects
    fn is_object_array(&self) -> bool;
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
    fn set_id(&mut self, id_iri: &str) {
        let map = self.as_object_mut().unwrap();
        map.insert("id".to_string(), Value::String(id_iri.to_string()));
    }
}
