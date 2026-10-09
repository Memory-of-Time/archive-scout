# v1.0.0 rate-limit and Windows GUI implementation

This document summarizes the release behavior introduced from the rate-limit and Windows GUI audit used to prepare the initial public release.

## Request control

- Index/CDX/Timemap: 2.5 seconds between actual attempts (24/minute).
- Replay: 0.125 seconds between actual attempts (8/second).
- Redirect hops and backend fallbacks are manually exposed to the transport admission hook and each count as a wire attempt.
- Shared cooldown: Retry-After is a minimum; missing headers start at 60 seconds or more with positive-only jitter.
- Recovery uses one probe and gradual adaptive-rate relaxation.
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
- Text/Listbox/Treeview wheel input remains local until an inner control reaches its boundary, then bubbles to the nearest outer scroll page.
- Results/FTS, History, Errors, and site-issue database reads run off the Tk thread with generation guards.

## Validation boundary

The repository contains offline regression tests for request accounting, cooldown policy, archived-origin handling, version identity, and Windows packaging metadata. Native Windows rendering, per-monitor movement, high-contrast visuals, touchpad behavior, and the final frozen executable still require smoke testing on a Windows machine because those behaviors cannot be fully exercised by a headless non-Windows test runner.
