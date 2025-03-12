# Getting Started

!!! warning
    Under construction 🚧

## Installation

For beginners, we recommend using our prebuilt containers available at
[quay.io](https://quay.io/repository/pinka/pinka).

## Configuration

The simplest configuration only requires a few options:

```toml
[activity_pub]
base_url = "https://social.example.org" # without trailing slash
webfinger_at_host = "@social.example.org"

[admin]
# Use long, hard to guess password. This is used to protect
# admin HTTP endpoints via basic auth.
password = "<secure password>"

[database]
# Pinka needs to persist some states. This folder should be able
# to survive reboots.
path = "/var/lib/pinka"
```

## Deployment

### Directly run the command

```bash
pinka serve -c config.toml
```

### Directly run the container

```bash
podman run -v ./config.toml:/etc/pinka/config.toml:z \
           -v pinka:/var/lib/pinka \
           quay.io/pinka/pinka serve -c /etc/pinka/config.toml
```

### Docker compose

TBA

### Podman quadlet

```ini
[Unit]
Description=Pinka Server
Wants=network-online.target

[Container]
ContainerName=pinka
Image=quay.io/pinka/pinka:latest
AutoUpdate=registry
NoNewPrivileges=true
ReadOnly=true
Tmpfs=/tmp
Volume=pinka:/var/pinka
Volume=/etc/pinka:/etc/pinka
Exec=serve -c /etc/pinka/config.toml

[Service]
Restart=on-failure
TimeoutStartSec=900

[Install]
WantedBy=default.target
```
