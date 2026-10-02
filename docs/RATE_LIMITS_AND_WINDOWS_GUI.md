# v1.0.1 rate-limit and Windows GUI implementation

## v1.0.1 corrective behavior

v1.0.1 makes a service deferral operation-wide instead of treating it as another malformed/slow page. It preserves pending request identity, stops sibling page admission, carries one absolute recovery incident deadline through the shared gate, and persists server eligibility across restarts. Adaptive pacing is incident-scoped.

For scrolling, focus reveal is visibility-aware and minimal, while one interpreter-wide wheel router prevents a single physical wheel event from moving both a native child and its outer page.

This document summarizes the v1.0.1 corrective behavior from the rate-limit and scrolling audit, while retaining the v1.0.0 request-control and Windows-DPI baseline.

## Request control

- Index/CDX/Timemap: 2.5 seconds between actual attempts (24/minute).
- Replay: 0.125 seconds between actual attempts (8/second).
- Redirect hops and backend fallbacks are manually exposed to the transport admission hook and each count as a wire attempt.
- Shared cooldown: Retry-After is a minimum; missing headers start at 60 seconds or more with positive-only jitter.
- Recovery uses one shared absolute incident deadline, one probe, restart-safe Retry-After eligibility, and gradual adaptive-rate relaxation applied once per coalesced incident.
- Typed `RateLimitDeferred` exceptions propagate through CDX fallback helpers unchanged.
- Historical origin 429/503 replay responses do not close the live Wayback host gate.
- The UI distinguishes rate-limit pause/resume advice from connectivity pause/resume advice.

## Windows/UI

- Windows packaging includes a Per-Monitor V2 DPI manifest and early DPI-awareness setup.
- User font scaling changes Tk named fonts, not the global `tk scaling` DPI baseline.
- Windows system dark mode and high-contrast mode are recognized.
- Initial/restored geometry is clamped to the current display.
- Sidebar and form-heavy pages use reusable scroll containers.
- Results, AI relevance, and Research Intelligence retain independent table scrollbars and adjustable detail panes while the outer page can scroll to otherwise-clipped controls.
- Text/Listbox/Treeview wheel input is routed once per physical event; an event that moves the native child to its boundary cannot also move the parent, while a later event at the boundary may bubble outward.
- Results/FTS, History, Errors, and site-issue database reads run off the Tk thread with generation guards.

## Validation boundary

The repository contains offline regression tests for request accounting, cooldown policy, archived-origin handling, version identity, and Windows packaging metadata. Native Windows rendering, per-monitor movement, high-contrast visuals, touchpad behavior, and the final frozen executable still require smoke testing on a Windows machine because those behaviors cannot be fully exercised by a headless non-Windows test runner.
