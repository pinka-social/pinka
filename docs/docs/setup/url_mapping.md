## Map Any URL to IRI

All pinka objects have this [IRI][iri] format:

    /as/objects/{uuid}

When the main website get a `application/activity+json` GET request we may wish
to return the corresponding Activity Streams object, there are several ways to
handle this request.

### Main website shares same domain with Pinka

This requires a reverse proxy to internally route the request to the pinka
server. In this setup, the website URL is transparently redirected to the pinka
resource IRI.

For example, this is how pinka.dev was set up using Caddy:

```
pinka.dev {
    @apub_query {
        header Accept "application/json"
        header Accept "application/jrd+json"
        header Accept "application/activity+json"
        header Accept "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\""
    }
    @apub_post {
        header Content-Type "application/json"
        header Content-Type "application/activity+json"
        header Content-Type "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\""
    }
    header >Vary Accept
    reverse_proxy @apub_query              pinka:8080
    reverse_proxy @apub_post               pinka:8080
    reverse_proxy /.well-known/webfinger*  pinka:8080
    reverse_proxy /.well-known/nodeinfo*   pinka:8080
    reverse_proxy /nodeinfo*               pinka:8080
    reverse_proxy /as/*                    pinka:8080
    reverse_proxy /users/*                 pinka:8080
    reverse_proxy /pinka/*                 pinka:8080
    reverse_proxy                          pinka-docs
}
```

It may be translated to Nginx like this:

```
server {
    server_name pinka.dev;

    location / {
        proxy_pass http://pinka-docs;
    }

    location ~* ^/(as|users|pinka|nodeinfo|\.well-known/(webfinger|nodeinfo)) {
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        add_header Vary Accept;
        proxy_pass http://pinka:8080;
    }

    location ~* ^/ {
        if ($http_accept ~* "application/(json|activity\\+json|ld\\+json|jrd\\+json)") {
            add_header Vary Accept;
            proxy_pass http://pinka:8080;
            break;
        }
    }
}
```

### Separate domain

In this setup, the pinka server has a different domain name than the main
website. The URL of the web page will still be set as the `url` property.

It's recommended configure content negoation to redirect activity or JSON-LD
requests to pinka to make federation work seamlessly.

[iri]: https://www.w3.org/TR/json-ld11/#iris