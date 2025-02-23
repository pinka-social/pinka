# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0-beta.1] - 2025-02-23

### 🚀 Features

- Allow binding to IPv6 address for IPv6 only node
- Add Containerfile
- Introduce Containerfile
- Use fully qualified image name
- *(apub)* Retry failed delivery
- *(apub)* Handle failure in inbox discovery
- *(apub)* Correctly retry both recipients and inboxes
- More robust delivery retry mechanism
- *(apub)* Implement replies endpoint
- *(apub)* Check both inReplyTo and context in S2sCreate
- *(apub)* Unwrap object from activity
- *(apub)* Attach reply counts to object
- Enhance threading and add replies collection
- *(apub)* Attach context and conversation to new objects
- *(apub)* Attach context and conversation to new objects
- *(apub)* Verify and accept all context from our own namespace
- *(http)* Allow CORS request
- *(http)* Implement basic comments rendering

### 🐛 Bug Fixes

- *(apub)* No need to copy actor to inner object
- *(raft)* Use correct min quorum match index
- *(apub)* Use http1 client for max compatibility
- *(apub)* Fix followers collection pagination

### 💼 Other

- *(deps)* Update dependencies
- Migrate to rust 2024 edition
- Migrate to rust 2024 edition

### 🚜 Refactor

- *(apub)* Log error details
- *(apub)* Do not send Accept header when POSTing
- *(raft)* Adjust default raft parameters
- *(raft)* Report inconsistent raft state
- Add error context to main function
- Improve supervisor error reporting
- Improve error reporting in cluster maint
- Improve raft error reporting
- Improve apub error reporting
- Improve http logging
- Improve error handling and logging
- Improve error logging

### 📚 Documentation

- Update FEATURES.md
- Update FEATURES.md
- Add design for comments rendering
- Update FEATURES.md

### ⚙️ Miscellaneous Tasks

- Add github workflow to build container
- Add github workflow to build container
- Install latest buildah and podman (non-default for arm64 runner)
- Use v2 redhat-actions/push-to-registry
- Use image name with registry namespace
- Create multi-arch manifest
- Build and push multi-arch container and manifest to quay.io
- Automatic build staging branch
- Add license header to js file

## [0.1.0-beta.0] - 2025-02-10

### 🚀 Features

- Initial commit
- Implement basic main loop
- Project scaffolding
- Implement basic supervision structure
- Implement append entries rpc message
- Implement request vote rpc message
- Implement Raft leader election
- Support remote server / cluster forming
- Implement log replication and client forwarding
- *(raft)* Implement observing only server for replicas
- Implement more robust cluster reconnect and crash recovery
- *(raft)* Fix raft vote request handling and isolate raft actor module
- *(apub)* Implement Actor store and Actor profile HTTP handler
- *(raft)* Implement Raft state machine interface
- *(apub)* Implement minimum client to server flow for Create activity
- *(apub)* Implement Activity Pub storage layer
- *(apub)* Implement followers collection endpoint
- *(apub)* Implement most basic inbox
- *(apub)* Implement most basic inbox activities
- *(apub)* Implement basic pagination support for collection
- *(apub)* Implement collections endpoint and pagination
- *(apub)* Add last property to outbox and followers collection
- *(apub)* Implement object retrieval endpoints
- *(apub)* Augment outbox objects with likes and shares
- *(apub)* Sort outbox in reverse chronological order
- *(apub)* Allow undo likes and follows
- *(apub)* Display likes and shares in outbox and handle undo
- *(apub)* Implement persisted delivery queue
- *(apub)* Adapt simple queue interface for raft
- *(apub)* Sketch delivery mechanism in state machine
- *(apub)* Use SystemTime to track visibility timeout
- *(apub)* Check local replicated queue before polling
- *(apub)* Implement basic delivery loop
- *(apub)* Implement foundation for outbox delivery
- *(apub)* Make simple queue fully deterministic with raft
- *(apub)* Make simple queue fully deterministic when using raft
- *(apub)* Support multiple queues in simple queue
- *(apub)* Implement collections addressing
- *(apub)* Implement delivery to collections
- *(apub)* Implement basic feed ingestion
- *(apub)* Implement basic feed ingestion
- *(apub)* Convert create to update when object already exists
- *(apub)* Schedule delivery after ingest feed
- *(apub)* Convert create to update when the object already exist
- *(raft)* Improve append_entries latency and throughput
- *(apub)* Generate RSA key pair when create new user
- *(apub)* Sign outgoing messages with actor key
- *(apub)* Verify incoming messages with HS2019
- *(apub)* Implement HTTP Signature signing and verification
- *(webfinger)* Implement basic webfinger
- *(webfinger)* Implement basic webfinger
- *(http)* Protect mutation endpoints with admin basic auth
- *(apub)* Return correct content-type header
- *(apub)* Warn unknown hs2019 algorithm label
- *(apub)* Fix followers collection pagination
- *(apub)* Implement basic Accept activity
- *(apub)* Post with rsa-sha256 as hs2019 algorithm
- *(apub)* Ensure accept activity has a id
- *(apub)* Actually queue Accept for delivery!
- *(apub)* Ensure actor property for ingested feed
- *(apub)* Give up deliver after retry 10 times
- *(apub)* Copy actor from object to activity
- *(apub)* Ensure update activity has id attached
- *(apub)* Ensure we check the id of id_obj_key
- *(apub)* Remove simulate mailman log - we post for real now
- *(apub)* Fixed some issues found in Mastodon federation testing

### 🐛 Bug Fixes

- *(raft)* Update_term should recognize the new leader directly
- *(raft)* Update_term should recognize the new leader directly
- *(apub)* Attach likes and shares to object instead of activity
- *(apub)* Fix test failures
- *(apub)* Count items correctly

### 💼 Other

- Switch to stable rust
- Use reqwest with rustls

### 🚜 Refactor

- Add version header to all postcard payload
- Add version header to all serialized data
- Keep Raft specific serde in the raft module
- Keep RaftWorker and RaftMsg module private
- *(config)* Move HttpConfig under server section
- *(apub)* Unify repo and index method to use batch
- *(apub)* Extract common IdObjIndex for ordered collections
- *(apub)* Simplify client to server Create activity handling
- *(apub)* Hide NodeValue from high level interface
- *(apub)* Base all Object operation on Object instead of json Value
- *(raft)* Fix clippy warnings
- *(apub)* Reuse index and repo objects
- *(apub)* Use object abstraction everywhere
- Flatten modules and move raft to top level
- *(apub)* Flatten modules layout
- Use spawn_blocking when possible
- Use spawn_blocking when possible
- *(raft)* Simplify persist mode and fix timeout caused by IO
- Simplify persist mode and avoid election caused by IO
- Move example configs to config file
- Reorganize dependency list
- *(raft)* Use batch to save raft state
- *(apub)* Remove unused ObjectRepo method all()
- *(apub)* Wrap key material with SecretBox
- Remove manhole actor and repl
- Remove raft-dump cli subcommand
- *(apub)* Keep only OrderedCollection
- Organize supervision and add keyspace maint task
- Remove unused config structs
- Remove currently unused methods
- *(apub)* Add extension @context

### 📚 Documentation

- Add rough implementation plan
- Add git-cliff config
- Add initial README and CONTRIBUTING documents
- Add initial README.md and CONTRIBUTING.md documents
- Add apub storage design
- Update implemented features
- Update CHANGELOG.md

### ⚙️ Miscellaneous Tasks

- Update gitignore and Cargo metdata

<!-- generated by git-cliff -->
