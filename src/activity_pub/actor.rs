use anyhow::{Error, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::config::ActivityPubConfig;

use super::{NodeValue, ObjectSerDe};

pub(crate) fn get_iri(config: &ActivityPubConfig, local_id: &str) -> String {
    format!("{}/users/{}", config.base_url, local_id)
}

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
    pub(crate) fn enrich_with(mut self, config: &ActivityPubConfig) -> Actor {
        let object = self
            .0
            .as_object_mut()
            .expect("Actor must be an JSON object");

        let base_url = &config.base_url;
        let id = object
            .get("id")
            .expect("Actor must have a local_id")
            .as_str()
            .expect("local_id must be a string");
        // TODO: correctly update @context
        let Value::Object(properties) = json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Person",
            "id": format!("{}/users/{}", base_url, id),
            "following": format!("{}/users/{}/following", base_url, id),
            "followers": format!("{}/users/{}/followers", base_url, id),
            "inbox": format!("{}/users/{}/inbox", base_url, id),
            "outbox": format!("{}/users/{}/outbox", base_url, id),
        }) else {
            unreachable!()
        };
        object.extend(properties);

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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ActivityPubConfig, Actor};

    #[test]
    fn enrich_actor() {
        let config = ActivityPubConfig {
            base_url: "https://social.example.com".to_string(),
        };
        let raw_actor = Actor(json!({
            "id": "john",
            "name": "John Smith",
            "icon": {
                "type": "Image",
                "mediaType": "image/jpeg",
                "url": "https://objects.social.example.com/493d7fea0a23.jpg"
            }
        }));
        let actor = raw_actor.enrich_with(&config);
        assert_eq!(
            actor,
            Actor(json!({
                "@context": "https://www.w3.org/ns/activitystreams",
                "type": "Person",
                "id": "https://social.example.com/users/john",
                "name": "John Smith",
                "following": "https://social.example.com/users/john/following",
                "followers": "https://social.example.com/users/john/followers",
                "inbox": "https://social.example.com/users/john/inbox",
                "outbox": "https://social.example.com/users/john/outbox",
                "icon": {
                    "type": "Image",
                    "mediaType": "image/jpeg",
                    "url": "https://objects.social.example.com/493d7fea0a23.jpg"
                }
            }))
        );
    }
}
