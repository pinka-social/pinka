//! ActivityPub data model
//!
//! JSON-LD or JSON?
//! * <https://socialhub.activitypub.rocks/t/activitypub-a-linked-data-spec-or-json-spec-with-linked-data-profile/3647/27>
//! * <https://g0v.social/@aud@fire.asta.lgbt/113386580058852762>
//!
//! `@context` handling in AP is default because the expansion and compaction
//! algorithm defined by JSON-LD is complex.
//!
//! I decided I will not implement full JSON-LD. I'll only parse the data to the
//! extend I can federiate with Mastodon.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub(crate) enum Kind {
    Actor(ActorKind),
    Multiple(Vec<String>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum ActorKind {
    Application,
    Group,
    Organization,
    Person,
    Service,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Actor {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) kind: Kind,
    pub(crate) name: String,
    #[serde(flatten)]
    pub(crate) others: BTreeMap<String, Value>,
}
