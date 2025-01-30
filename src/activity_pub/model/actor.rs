use anyhow::{Error, Result, bail};
use serde_json::{Value, json};

use crate::config::ActivityPubConfig;

use super::Object;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Actor(Value);

impl TryFrom<Object> for Actor {
    type Error = Error;

    fn try_from(object: Object) -> Result<Self, Self::Error> {
        let value = object.as_ref();
        if value.get("id").is_none() {
            bail!("actor should have id property");
        }
        if value.get("name").is_none() {
            bail!("actor should have name property");
        }
        Ok(Actor(object.0))
    }
}

impl AsRef<Value> for Actor {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

impl AsMut<Value> for Actor {
    fn as_mut(&mut self) -> &mut Value {
        &mut self.0
    }
}

impl From<Actor> for Value {
    fn from(value: Actor) -> Self {
        value.0
    }
}

impl Actor {
    // TODO
    pub(crate) fn enrich_with(mut self, config: &ActivityPubConfig) -> Self {
        let map = self
            .as_mut()
            .as_object_mut()
            .expect("Actor must be an JSON object");

        let base_url = &config.base_url;
        let id = map
            .get("id")
            .expect("Actor must have a local_id")
            .as_str()
            .expect("local_id must be a string");
        // TODO: correctly update @context
        let Value::Object(properties) = json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Person",
            "id": format!("{}/users/{}", base_url, id),
            "followers": format!("{}/users/{}/followers", base_url, id),
            "inbox": format!("{}/users/{}/inbox", base_url, id),
            "outbox": format!("{}/users/{}/outbox", base_url, id),
        }) else {
            unreachable!()
        };
        map.extend(properties);

        self
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;

    use crate::activity_pub::model::Object;

    use super::{ActivityPubConfig, Actor};

    #[test]
    fn enrich_actor() -> Result<()> {
        let config = ActivityPubConfig {
            base_url: "https://social.example.com".to_string(),
        };
        let object = Object::try_from(json!({
            "id": "john",
            "name": "John Smith",
            "icon": {
                "type": "Image",
                "mediaType": "image/jpeg",
                "url": "https://objects.social.example.com/493d7fea0a23.jpg"
            }
        }))?;
        let actor = Actor::try_from(object)?.enrich_with(&config);
        assert_eq!(
            actor.as_ref(),
            &json!({
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
            })
        );
        Ok(())
    }
}
