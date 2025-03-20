## Installation

### From pre-built container

```bash
podman pull quay.io/pinka/pinka
```

### From cargo

```bash
cargo install pinka
```

## Deployment

Pinka is a single executable. At run time it needs a storage folder and a
configuration file.

### Docker compose

```yaml
version: '3.8'

services:
    pinka:
        image: quay.io/pinka/pinka:latest
        container_name: pinka
        restart: unless-stopped
        read_only: true
        tmpfs:
            - /tmp
        volumes:
            - pinka_data:/var/lib/pinka
            - /etc/pinka:/etc/pinka:ro
        command: serve -c /etc/pinka/config.toml

volumes:
    pinka_data:
        driver: local
```

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
Volume=pinka_data:/var/lib/pinka
Volume=/etc/pinka:/etc/pinka
Exec=serve -c /etc/pinka/config.toml

[Service]
Restart=on-failure
TimeoutStartSec=900

[Install]
WantedBy=default.target
```

### Systemd unit

```ini
[Unit]
Description=Pinka Server
After=network.target

[Service]
ExecStart=/usr/bin/pinka serve -c /etc/pinka/config.toml
Restart=on-failure
User=pinka
Group=pinka
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
StateDirectory=pinka
ConfigurationDirectory=pinka
TimeoutStartSec=900

[Install]
WantedBy=multi-user.target
```