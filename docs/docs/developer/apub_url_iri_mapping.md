## Map Any URL to IRI

All pinka objects have this IRI format:

    /as/objects/{uuid}

When the main website get a `application/activity+json` GET request, there are
two possible ways to handle this request.

### Setup 1 - Reverse Proxy

If there is a reverse proxy it can internally route the request to the pinka
server. In this setup, the website URL is redirected to the pinka resource IRI.

When posting activities, the object should use the original website URL as its
`url` property.

Pinka handles this with the root path handler.

### Setup 2 - Separate Domain

In this setup, the pinka server has a different domain name than the main
website. The original URL will still be set as the `url` property. When the
pinka object IRI is dereferenced with HTML content negoation, pinka will
redirect the user-agent to the `url`.
