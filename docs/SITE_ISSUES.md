# Site-specific Wayback issues

Archive failures are more useful when the researcher can tell a permanent archive-policy restriction from a temporary network problem.

Archive Scout records site issues by host, stage, category, status, occurrence count, and last-seen time.

## Categories

- Wayback exclusion
- robots.txt restriction
- missing capture
- invalid Wayback replay
- Wayback access restriction
- archived origin unavailable
- rate limit
- timeout
- connection problem
- TLS/certificate problem
- Wayback client/server HTTP errors

Wayback sometimes returns an explanatory HTML page with HTTP 200. Archive Scout inspects bounded response text for known exclusion, robots, missing-capture, and archived-origin markers so those pages are not mistaken for successful captures.

## Behavior

Transient conditions remain retryable according to project settings. Confirmed exclusion and robots conditions are nonretryable and can form a host-level circuit for media work, preventing repeated futile requests while the project continues other available targets.

The program reports these conditions; it does not attempt to bypass archive restrictions.
