## Dynamic

Pinka provides a simple JavaScript that can fetch replies and insert comments
into the page.

First, include the JavaScript file:

```html
<script src="https://example.com/pinka/comments.js"></script>
```

Then specify where the comments will be added:

```html
<div class="pinka-comments"></div>
```

The script will perform the following steps:

1. Append a style sheet to `<head>`.
2. Wait until the document is loaded.
3. Fetch the current page URL with `Accept: application/ld+json;
   profile="https://www.w3.org/ns/activitystreams"`, triggering the Pinka server
   to return the object of the current page.
4. Abort if the object is not found.
5. Fetch the object's `replies` collection.
6. Pass the fetched collection to a callback for rendering.

The default callback will:

1. Find the first `div` element with the `pinka-comments` class.
2. For each reply in the replies collection, append a comment block to the
   parent `div`.
3. Append a button to load next page.

Both the style sheet and the rendering callback can be customized:

```js
Pinka.setStyleFn(myStyleCallback);
Pinka.setRenderFn(myRenderCallback);
```

## Static

Similar steps can be done for static rendering if you want zero JavaScript.

Static site generators like Jekyll or Zola can include dynamic content by
fetching JSON objects from HTTP endpoints.

For each page, fetch the page URL to get the page object in JSON, then fetch the
replies collection and render them. This can be scheduled in a CI or cron job to
periodically update the comments section.

TODO: Verify if pagination can be easily done, otherwise provide a dedicated
endpoint for static site generators.
