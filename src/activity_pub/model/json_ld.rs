use serde_json::{Value, json};

pub(crate) enum JsonLdContext {
    ActivityStreams,
    SecurityV1,
    OStatus,
    Toot,
}

impl JsonLdContext {
    pub(crate) fn to_json(&self) -> Value {
        match self {
            JsonLdContext::ActivityStreams => json!("https://www.w3.org/ns/activitystreams"),
            JsonLdContext::SecurityV1 => json!("https://w3id.org/security/v1"),
            JsonLdContext::OStatus => json!({
                "ostatus": "http://ostatus.org#",
                "conversation": "ostatus:conversation",
            }),
            JsonLdContext::Toot => json!({
                "toot": "http://joinmastodon.org/ns#",
                "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
                "discoverable": "toot:discoverable",
                "indexable": "toot:indexable"
            }),
        }
    }
}
