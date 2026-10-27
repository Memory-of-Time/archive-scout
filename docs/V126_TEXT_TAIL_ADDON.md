# v1.2.6 text retry-tail add-on

Apply this add-on **after** ArchiveScout-v1.2.6-Run-Finishing-and-Dashboard-Patch.zip. It is a delta against that delivered patch, not a replacement for it. Application version 1.2.6 and SQLite schema 11 are unchanged.

## Observed behavior

The SteakAndCheese reference log is Pasted text(20261027-220251).txt, build 1.2.6:5ac15cd1b9fe39c3. Between approximately 15:19 and 17:22 it records no additional successful saves, holding at 217,206. It contains 818 queue preparations. Incomplete-representation failures increase by 831 and oversized-response failures by 680 between the first and last health samples. Most visible retry batches contain one to three captures.

These are attempt counters, not counts of distinct broken captures. The log alone does not establish which captures produced both categories, why a representation was incomplete, or whether a newer patch was installed. Queue preparation usually takes only about 15–47 ms in the displayed samples. A queue/pool rewrite is not justified by these timings.

## Reproduced defect and change

A text failure classified as nonretryable removed its retry schedule. Several automatic selectors then treated an absent schedule plus any older unresolved retryable error as eligible. A new size-limit refusal or other nonretryable outcome could therefore be repeatedly retried because of historical connection/transfer errors.

The current owner's latest decision is now persisted in the existing capture routing record. Automatic initial acquisition, interleaved retry admission, recovery sweeps, and next-due checks all honor it. Error records, their original categories/retryability, attempt counts and payload evidence are preserved. No ambiguous timestamp ordering of historical errors is used to infer the latest outcome.

Held captures remain in state error. They remain counted as failed/unresolved captures and prevent a complete operation result. Holding automatic replay attempts does not mark their contents recovered, excluded, or permanently unavailable.

A size-limit hold records the actual configured byte limit that caused it. Raising that limit makes the affected captures pending again within the selected site/date/signature scope. Other targets can be revisited later using their own stored limits. Unchanged or smaller limits do not restart the same failure. A newer nonretryable outcome, such as a 404, supersedes an earlier size-limit hold. Scope checks include overlapping target membership.

An explicit selected-error retry can recheck a size-limit or missing-capture outcome once. Automatic follow-up recovery still honors the new result, even while that explicit selection remains configured. Text and media selected-error paths use the same categories for this behavior; robots/exclusion policy failures are not reopened by the added size-limit exception. Unselected Retry Errors and connection-category retries also honor the latest hold. The media Retry Errors path now honors its existing persisted latest-decision record, preventing an older transient error from reviving a newer nonretryable media outcome.

Incomplete transfers remain retryable. They use the bounded, progress-sensitive recovery budget from the preceding patch. This add-on does not relax complete-body, encoding, range or digest validation. There is no new adaptive pacing or general retry delay. Healthy replay pacing and worker settings remain unchanged.

## Storage and resource behavior

No permanent table or column is added. The existing free-text routing field carries the latest decision and, for a size-limit hold, its configured limit. Existing classification kind, confidence and evidence fields are preserved. Policy reactivation uses a temporary disk-backed ID table and indexed error-state selection; it does not load the project history into Python or scan every routing row on each capture.

## Validation

New regressions cover historical-error revival, all automatic selection paths, reopen and rollback behavior, retained error/attempt history, classification preservation, scoped size changes, later target selection, explicit retries, and real localhost oversized/truncated response fixtures. The oversized fixture remains unfinished at the small limit, makes no automatic request on the next run, and saves the exact full body after the limit is increased. The truncated fixture remains unsaved and retryable after the operation's bounded recovery attempts.

The package's VALIDATION directory contains exact test results and reproducible before/after synthetic eligibility measurements at 100,000, 500,000 and 1,000,000 history captures. These demonstrate eligibility and local costs, not live Internet Archive throughput or day-long stability. Linux/Python 3.12 is exercised here; native Windows/macOS and display-dependent GUI behavior remain unverified in this environment.
