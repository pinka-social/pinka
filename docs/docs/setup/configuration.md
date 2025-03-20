Configuration is done through a config file in TOML format. The complete config
options can be found in the [reference](../reference/configuration.md).

Use the command line option `-c` or `--config` to specify the location of the
config file.

```
pinka run -c /etc/pinka/config.toml
```

## Activity Pub

Pinka needs to know the base URL in order to construct the full actor URL or
object URL. It also needs to know the webfinger host so federated server can use
webfinger to discover local actors, for example `@user@social.example.org` on
Mastodon.

```toml
[activity_pub]
base_url = "https://social.example.org"
webfinger_at_host = "social.example.org"
```

## Feed

Pinka can periodically parse RSS/Atom/JSON feeds and publish new feed items as
activity. Multiple feeds can be configured and attribute to different users. The
feed content can be transformed or decorated with a [minijinja][] template.

```toml
[feeds.social-example-org]
uid = "username"
feed_url = "https://social.example.org/feed.xml"
base_url = "https://social.example.org"
template = '''
<p>{{ title.content|to_text }}</p>

<p>{{ content.body|excerpt(100) }}</p>

{% if id is startingwith "http" %}
<p><a href="{{ id }}">{{ id }}</a></p>
{% elif links %}
<p><a href="{{ links[0].href }}">{{ links[0].href }}</a></p>
{% endif %}
'''
```

In addition to the builtin minijinja filters, two custom filters are available:

* `to_text(string)` strips all HTML tags and convert the content to plain text.
* `excerpt(string, n)` Truncate a string to a certain number of words. Each CJK
  character is counted as one word. Last word is not truncated.

[minijinja]: https://docs.rs/minijinja/latest/minijinja/syntax/index.html