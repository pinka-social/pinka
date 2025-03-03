# Configuration Reference

!!! warning
    Under construction 🚧

This document provides a reference for the configuration options available.

## ActivityPub Configuration
```toml
[activity_pub]
base_url = "https://your-base-url"
webfinger_at_host = "your-webfinger-host"
```

## HTTP Configuration
```toml
[http]
listen = true
address = "[::1]"
port = 8080
```

## Admin Configuration
```toml
[admin]
password = "your-secret-password"
```

## Raft Configuration
```toml
[raft]
heartbeat_ms = 100
min_election_ms = 1000
max_election_ms = 2000
```

## Cluster Configuration
```toml
[cluster]
auth_cookie = "your-auth-cookie"
use_mtls = true
pem_dir = "/path/to/pem_dir"
ca_certs = ["/path/to/ca_cert1", "/path/to/ca_cert2"]
servers = []
reconnect_timeout_ms = 5000
```

## Server Configuration
```toml
[cluster.servers.<server-name>]
hostname = "server-hostname"
port = 8080
readonly_replica = false
server_ca_certs = ["/path/to/server_ca_cert1", "/path/to/server_ca_cert2"]
server_cert_chain = ["/path/to/server_cert1", "/path/to/server_cert2"]
server_key = "/path/to/server_key"
client_ca_certs = ["/path/to/client_ca_cert1", "/path/to/client_ca_cert2"]
client_cert_chain = ["/path/to/client_cert1", "/path/to/client_cert2"]
client_key = "/path/to/client_key"
http = { listen = true, address = "[::1]", port = 8080 }
```

## Database Configuration
```toml
[database]
path = "/path/to/database"
```
