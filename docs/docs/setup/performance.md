# Performance

## Using Read-Only Replication

Read-only replication can enhance performance by distributing read operations
across multiple replicas, reducing the load on the primary nodes.

Pinka cluster nodes can be configured as replication-only. A replication-only
node does not participate in quorum or leader election. It's recommended to keep
the number of primary nodes small to maintain low write latency.

### Steps to Implement Read-Only Replication:

1. Set up a few primary nodes and one or more read replicas follow the [high
   availability setup](high-availability.md).
2. Configure your DNS or load balancer to direct read queries to the replicas
   and write queries to the primary nodes.
3. Monitor the performance and health of both the primary and replica databases.

!!! note

    Since read-only replicas do not participate in quorum, there is a chance they may fall far behind in replication and serve stale data.

## Using CDN

Content delivery networks (CDNs) can enhance the performance of Pinka by caching
static assets and activity stream objects closer to your users. This reduces
latency and speeds up load times. Usually, a CDN is not needed, but sites with
very high traffic may consider this.

### Steps to Implement CDN:

1. Choose a CDN provider (e.g., Cloudflare, AWS CloudFront).
2. Configure your CDN settings to cache static assets such as images, CSS,
   JavaScript files, and activity stream objects.
3. Update your DNS settings to point to the CDN.
4. Test Pinka to ensure assets are being served from the CDN.
5. Set a short cache time for dynamic content like activity stream objects to
   ensure users receive the most up-to-date information.

!!! note

    Activity Streams objects have the content-type `application/activity+json` or `application/ld+json; profile="https://www.w3.org/ns/activitystreams"`.
