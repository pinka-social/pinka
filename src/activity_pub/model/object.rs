//! Storage friendly presentation of Activity Streams' core data model.

use anyhow::{Result, bail};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Object(Value);

impl TryFrom<Value> for Object {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self> {
        if !value.is_object() {
            bail!("value is not a JSON object");
        }
        Ok(Object(value))
    }
}

impl_object_serde_new_type!(Object);

impl Object {
    pub(crate) fn is_activity(&self) -> bool {
        if let Some(Value::String(typ)) = self.0.get("type") {
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
}
