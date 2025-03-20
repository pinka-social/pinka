# Getting Started

## Installation

For beginners, we recommend using our prebuilt containers available at
[quay.io](https://quay.io/repository/pinka/pinka).

## Configuration

The simplest configuration only requires a few options:

```toml
[activity_pub]
base_url = "https://social.example.org" # without trailing slash
webfinger_at_host = "@social.example.org"

[cluster.servers.single]

[admin]
# Use long, hard to guess password. This is used to protect
# admin HTTP endpoints via basic auth.
password = "<secure password>"

[database]
# This folder should be able to survive reboots.
# Use a persisted volume when using containers.
path = "/var/lib/pinka"
```

## Run

```bash
podman run -v ./config.toml:/etc/pinka/config.toml:z \
           -v pinka:/var/lib/pinka \
           -p 8080:8080 \
           quay.io/pinka/pinka serve -c /etc/pinka/config.toml
```

## Create new user

Create a JSON file with the user profile:

```json
{
    "type": "Person",
    "id": "example",
    "preferredUsername": "example",
    "name": "User Example",
    "summary": "This is a example actor, we can use any Actor properties",
    "url": "https://social.example.org",
    "discoverable": true,
    "manuallyApprovesFollowers": false
}
```

Then post to the user profile to create this actor. We set `gen_rsa` to true to
initialize the actor's HTTP signature cryptography. We only need to set it once.

```bash
curl -u pinka --json @user.json \
    http://localhost:8080/users/example\?gen_rsa=true
```

Now you can use tools like <https://browser.pub/http://localhost:8080/users/example> to expolore the activities.

Next check the user guide [Setup](./setup/index.md) and
[Configuration](./setup/configuration.md) for detailed setup and configuration
options.