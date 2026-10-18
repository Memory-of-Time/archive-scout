# External embedded-media pipeline

The external embedded-media operation is designed for research where a site contains archived pages that reference images or videos hosted elsewhere.

## Sequence

1. Index the specified text target.
2. Download and parse its textual captures.
3. Run selected keyword scans and normal reports.
4. Discover image/video references from the downloaded pages.
5. Persist those URLs in the media discovery queue.
6. Resolve exact Wayback captures for the discovered URLs.
7. Download the archived media with the normal media downloader.

This ordering is intentional: media discovery uses the actual downloaded pages rather than guessing external resources before page content has been preserved.

## Discovery coverage

Archive Scout recognizes standard and historical patterns including:

- `img` source and lazy-source attributes;
- `srcset` and lazy `srcset` candidates;
- `video`, `source`, and poster URLs;
- `object`, `embed`, FlashVars, and legacy player configuration;
- CSS `url(...)` references and background attributes;
- Open Graph and Twitter image/video metadata;
- image/video preload hints;
- direct media links;
- Wayback replay URLs embedded inside archived HTML;
- extensionless URLs when the HTML context identifies the candidate as an image or video.

Extensionless candidates are resolved through CDX metadata before final media classification.

## Performance

Discovery is incremental and persistent. A source document's content hash prevents unchanged pages from being reparsed for the same media-query signature. Local HTML extraction uses bounded workers, while all SQLite writes remain on the owning thread.

Exact Wayback lookups use bounded concurrency but share the same request-start limiter as the rest of Archive Scout. This overlaps network latency without increasing the configured request frequency.

Downloads stream directly to disk and hash incrementally. Known smaller captures are processed first.

## Unavailable hosts

When Wayback identifies an external host as excluded or blocked by robots restrictions, Archive Scout records the condition and avoids repeatedly querying or downloading thousands of other candidates from the same host during the project. The condition is visible in the Errors page and can be marked resolved by the researcher if archive availability changes later.
