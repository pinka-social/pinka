use serde_json::{Value, json};

use crate::config::ActivityPubConfig;

use super::Object;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Actor<'a>(Object<'a>);

impl<'a> From<Object<'a>> for Actor<'a> {
    fn from(object: Object<'a>) -> Self {
        Actor(object)
    }
}

impl Actor<'_> {
    // TODO
    pub(crate) fn enrich_with(self, config: &ActivityPubConfig, public_key_pem: &str) -> Self {
        let base_url = &config.base_url;
        let id = self.0.id().expect("Actor should have an IRI id");

        // TODO: correctly update @context
        let Value::Object(properties) = json!({
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                "https://w3id.org/security/v1",
                {
                    "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
                    "toot": "http://joinmastodon.org/ns#",
                    "discoverable": "toot:discoverable",
                    "indexable": "toot:indexable"
                }
            ],
            "type": "Person",
            "id": format!("{}/users/{}", base_url, id),
            "followers": format!("{}/users/{}/followers", base_url, id),
            "inbox": format!("{}/users/{}/inbox", base_url, id),
            "outbox": format!("{}/users/{}/outbox", base_url, id),
            "publicKey": {
                "id": format!("{}/users/{}#main-key", base_url, id),
                "owner": format!("{}/users/{}", base_url, id),
                "publicKeyPem": public_key_pem
            }
        }) else {
            unreachable!()
        };
        Actor(self.0.augment_with(properties))
    }
}

impl From<Actor<'_>> for Value {
    fn from(value: Actor<'_>) -> Self {
        value.0.to_value()
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
            webfinger_at_host: "@social.example.com".to_string(),
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
        let actor = Actor::from(object).enrich_with(&config, "PEM");
        assert_eq!(
            actor,
            Actor(Object::from(&json!({
                "@context": [
                    "https://www.w3.org/ns/activitystreams",
                    "https://w3id.org/security/v1"
                ],
                "type": "Person",
                "id": "https://social.example.com/users/john",
                "name": "John Smith",
                "followers": "https://social.example.com/users/john/followers",
                "inbox": "https://social.example.com/users/john/inbox",
                "outbox": "https://social.example.com/users/john/outbox",
                "publicKey": {
                    "id": "https://social.example.com/users/john#main-key",
                    "owner": "https://social.example.com/users/john",
                    "publicKeyPem": "PEM"
                },
                "icon": {
                    "type": "Image",
                    "mediaType": "image/jpeg",
                    "url": "https://objects.social.example.com/493d7fea0a23.jpg"
                }
            })))
        );
        Ok(())
    }
}
