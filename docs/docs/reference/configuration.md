# Configuration Reference

This document provides a reference for the configuration options available.

## ActivityPub Configuration
```toml
[activity_pub]
base_url = "https://your-base-url"
webfinger_at_host = "your-webfinger-host"
```

`base_url`: The base URL used to construct actor or object URLs. (e.g. `{base_url}/users/{username}/followers`)

`webfinger_at_host`: The host name of a webfinger account. (e.g. `example.org` in `@user@example.org`)

## Admin Configuration
```toml
[admin]
password = "your-secret-password"
```

`password`: The password used for admin endpoint authentication via HTTP basic auth.

## Raft Configuration
```toml
[raft]
heartbeat_ms = 100
min_election_ms = 1000
max_election_ms = 2000
```

`heartbeat_ms`: Interval between Raft heartbeats. It should be much shorter than the election timeout.

`min_election_ms`: The minimum timeout before a node may start a new leader
election. It should be at least 5x or 10x round-trip latency between nodes.

`max_election_ms`: The maximum timeout before a node may start a new leader
election. It should be at least 5x or 10x round-trip latency between nodes.

## Cluster Configuration
```toml
[cluster]
auth_cookie = "your-auth-cookie"
use_mtls = true
pem_dir = "/path/to/pem_dir"
ca_certs = ["/path/to/ca_cert1", "/path/to/ca_cert2"]
reconnect_timeout_ms = 5000
```

`auth_cookie`: A short string to ensure each node is using the same
configuration.

`use_mtls`: Enable transport layer security on connection between nodes.
Recommended for WAN setup.

`pem_dir`: The base directory path for certificates used by mTLS. Required if
`use_mtls` is true.

`ca_certs`: List of CA certificate files relative to `pem_dir`.

`reconnect_timeout_ms`: Time before a node may retry a broken connection to
other nodes.

## Server Configuration
```toml
[cluster.servers.<server_name>]
hostname = "server-hostname"
port = 8001
readonly_replica = false
server_ca_certs = ["/path/to/server_ca_cert1", "/path/to/server_ca_cert2"]
server_cert_chain = ["/path/to/server_cert1", "/path/to/server_cert2"]
server_key = "/path/to/server_key"
client_ca_certs = ["/path/to/client_ca_cert1", "/path/to/client_ca_cert2"]
client_cert_chain = ["/path/to/client_cert1", "/path/to/client_cert2"]
client_key = "/path/to/client_key"
http = { listen = true, address = "[::1]", port = 8080 }
```

`hostname`: Domain name or IP address of this server.

`port`: The TCP port number for inter node communication.

`readonly_replica`: Set to true to create a readonly node that does not
participant leader election or quorum.

`server_ca_certs`: List of CA certificates specific to this server.

`server_cert_chain`: Public key certificate of this server.

`server_key`: The private key of this server.

`client_ca_certs`: List of CA certificates specific to this node as a client.

`client_cert_chain`: Public key certificate of this node as a client.

`client_key`: The private key of this node as a client.

`http`: Configure the HTTP service for Activity Pub.

## Database Configuration
```toml
[database]
path = "/path/to/database"
```

`path`: Path to the base directory of all database files. Each server will store their state in the `{database.path}/{server_name}/` folder.

## Feed Configuration

```toml
[feeds.<feed_name>]
uid = "username"
feed_url = "URL"
base_url = "URL"
template = '''
<p>{{ title.content|to_text }}</p>
{% if summary %}
<p>{{ summary.content|to_text|excerpt(400) }}</p>
{% else %}
<p>{{ content.body|to_text|excerpt(400) }}</p>
{% endif %}
{% if id is startingwith "http" %}
<p><a href="{{ id }}">{{ id }}</a></p>
{% elif links %}
<p><a href="{{ links[0].href }}">{{ links[0].href }}</a></p>
{% endif %}"
'''
```

`uid`: All the feed activity will be attributed to this actor.

`feed_url`: The URL of the feed. Pinka supports RSS/Atom/JSON feeds.

`base_url`: The URL used to resolve relative URLs in the feed.

`template`: The template used to format the activity content.