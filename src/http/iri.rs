use crate::ActivityPubConfig;

pub(super) fn get_actor_iri(config: &ActivityPubConfig, local_id: &str) -> String {
    format!("{}/users/{}", config.base_url, local_id)
}
