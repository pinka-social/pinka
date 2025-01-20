use anyhow::{Error, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::RuntimeConfig;

use super::{NodeValue, ObjectSerDe};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct Actor(pub(crate) Value);

impl TryFrom<Value> for Actor {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        if value.get("id").is_none() {
            bail!("actor should have id property");
        }
        if value.get("name").is_none() {
            bail!("actor should have name property");
        }
        Ok(Actor(value))
    }
}

impl Actor {
    pub(crate) fn enrich_with(self, config: &RuntimeConfig) -> Actor {
        // TODO
        self
    }
}

impl ObjectSerDe for Actor {}

impl From<Actor> for NodeValue {
    fn from(value: Actor) -> Self {
        value.0.into()
    }
}

impl From<NodeValue> for Actor {
    fn from(value: NodeValue) -> Self {
        Actor(value.into())
    }
}

impl From<Actor> for Value {
    fn from(value: Actor) -> Self {
        value.0
    }
}
