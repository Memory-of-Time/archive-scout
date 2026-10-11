# Resilient indexing

Resilient indexing combines persistence, bounded concurrency, and failure classification.

Key behaviors:

- request signatures prevent incompatible runs from sharing state;
- page/resume progress is committed to SQLite;
- completed work is not repeated after restart;
- large response parsing uses compact row forms and bounded resident buffers;
- oversized or repeatedly slow intervals can become smaller resumable windows;
- persistent connection failures pause rather than cascading through every endpoint;
- 429 responses coordinate workers through a shared gate;
- permanent archive-policy failures are separated from transient transport failures;
- repeated identical CDX rows avoid unnecessary database rewrites.

The goal is forward progress with preserved state, not aggressive retrying.
