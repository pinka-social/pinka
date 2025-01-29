//! Storage friendly presentation of Activity Streams' core data model.

use anyhow::{Result, bail};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Object(pub(super) Value);

impl<'a> TryFrom<Value> for Object {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self> {
        if !value.is_object() {
            bail!("value is not a JSON object");
        }
        Ok(Object(value))
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
