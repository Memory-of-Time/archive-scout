# Archive Scout 1.0.1 release notes

Archive Scout 1.0.1 is a focused corrective release for rate-limit recovery and GUI scrolling. The project schema remains **11**.

## Wayback recovery

An exhausted live 429/503 recovery budget is now a typed, resumable service pause. Text and media indexers preserve the exact pending request and propagate that pause to the operation boundary without reducing row caps, subdividing date windows, rotating endpoints, switching formats, or granting a fresh automatic recovery budget.

Paged schedulers treat the first service deferral as a pool-wide control signal: new page admission stops, queued sibling work is cancelled, already-validated pages are committed with their checkpoints, and unfinished pages remain pending without being counted as ordinary page failures.

The shared host gate now carries an absolute recovery deadline across workers. Time already spent behind a shared cooldown counts toward that deadline. Retry-After remains a minimum server deadline and its wall-clock eligibility is persisted so restarting Archive Scout cannot cause an early retry. Adaptive pacing changes at most once per coalesced incident, and healthy automatic indexing continues resume-key traversal rather than switching a dense first response to paged mode.

## Scrolling and Windows interaction

Focusing a control that is already visible no longer changes the page position. Keyboard traversal reveals an off-screen control only as far as necessary, using the actual canvas viewport and coalesced idle work without calling `update_idletasks()` from the focus handler.

Wheel input is routed once through a single interpreter-wide router. Routing prefers the surface under the pointer, retains high-resolution residuals per stable surface/axis, and prevents a native Text/Listbox/Treeview that reaches its boundary from also moving its parent on the same event.

## Compatibility

The public application version is **1.0.1** and the internal project schema remains **11**. Existing v1.0.0 projects remain compatible.
