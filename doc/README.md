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

* Likes
* Shares

[AS2]: https://www.w3.org/TR/activitystreams-core/
[AP]: https://www.w3.org/TR/activitypub/