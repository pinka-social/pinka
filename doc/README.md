## High Level Diagram

![Pinka Components](./pink2-components.png)

## Data Model

The model is based on [Activity Streams (AS) 2.0][AS2] and [Activity Pub (AP)][AP].

### Collections

**Per AS Actor**

* Outbox
    * Lists the activities of an actor.
        * Each activity contains or references other AS objects.
        * Can be served from Raft followers with read-committed consistency.
* Inbox
    * Virtually contains all activities received by an AS actor.
        * Activities POSTed to the inbox is added to the Raft leader's log.
* Followers
* Following
* Liked

**Per AS Object**

Indexed by object id
Optionally index by actor id?

* Likes
* Shares

[AS2]: https://www.w3.org/TR/activitystreams-core/
[AP]: https://www.w3.org/TR/activitypub/

## References

* <https://www.w3.org/TR/activitypub/>
* <https://www.w3.org/TR/activitystreams-core/>
* <https://www.w3.org/TR/activitystreams-vocabulary/>
* <https://www.w3.org/ns/activitystreams>
* <https://www.w3.org/TR/json-ld1/>
* <https://www.w3.org/TR/json-ld11/>
* <https://www.w3.org/TR/json-ld11-api/>
* <https://docs.joinmastodon.org/spec/activitypub/>
* <https://socialhub.activitypub.rocks/t/activitypub-a-linked-data-spec-or-json-spec-with-linked-data-profile/3647>
* <https://g0v.social/@aud@fire.asta.lgbt/113386580058852762>
* <https://socialhub.activitypub.rocks/t/guide-for-new-activitypub-implementers/479>
* <https://www.jeremydormitzer.com/blog/more-than-json.html>
* <https://crates.io/crates/json-ld>
* <https://crates.io/crates/activitypub_federation>
* <https://raft.github.io/>
* <https://github.com/madsim-rs/madsim>
* <https://github.com/rabbitmq/ra/blob/main/docs/internals/INTERNALS.md>
* <https://eli.thegreenplace.net/2020/implementing-raft-part-2-commands-and-log-replication/>
* <https://www.w3.org/wiki/ActivityPub/Primer>
* <https://swicg.github.io/activitypub-http-signature/>
* <https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures>
* <https://www.rfc-editor.org/rfc/rfc9421>
* <https://stackoverflow.com/questions/75351338/pkcs1-vs-pkcs8-vs-pkcs12-for-rsa-keys>
