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

### Separate domain

In this setup, the pinka server has a different domain name than the main
website. The URL of the web page will still be set as the `url` property.

It's recommended configure content negoation to redirect activity or JSON-LD
requests to pinka to make federation work seamlessly.

[iri]: https://www.w3.org/TR/json-ld11/#iris