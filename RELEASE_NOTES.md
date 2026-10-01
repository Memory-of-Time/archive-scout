# Archive Scout 1.0.0 release notes

Archive Scout 1.0.0 is the initial public release. It combines the validated acquisition, classification, scanning, media, report, recovery, research, and automation systems into one release identity.

## Wayback request control

Index/CDX/Timemap traffic starts at one actual request attempt every 2.5 seconds (24/minute), while replay traffic starts at one attempt every 0.125 seconds (8/second). The clocks are shared per process and do not accumulate burst credit while idle. Redirect hops and fallback transport attempts pass through the same admission path.

Live 429/503 responses close the shared Wayback gate. Retry-After seconds and HTTP-date values are minimum deadlines; without a usable header, the first pause is at least 60 seconds with positive-only jitter. Recovery requires a probe and pacing relaxes gradually after sustained healthy traffic. Historical origin statuses returned through replay captures do not close the live gate.

## Windows and GUI

Windows builds declare Per-Monitor V2 DPI awareness. Archive Scout leaves Tk's system DPI baseline intact and applies the user font preference through named fonts. System theme detection recognizes Windows dark mode and high contrast. Main pages use reusable scroll containers, sidebar navigation scrolls independently, wide tables expose both axes, and nested wheel routing hands control from Text/Tree widgets to outer pages only at their boundaries.

Potentially heavy Results/FTS, History, Errors, and site-issue database reads run outside the Tk event loop and use generation tokens so stale queries cannot overwrite newer UI state.

## Compatibility

The public application version is 1.0.0 and the internal project schema remains 11. The loader accepts identifiers emitted by pre-release development builds so existing Archive Scout project directories can be opened and migrated rather than abandoned.
