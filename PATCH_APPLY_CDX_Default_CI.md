# Scout 1.2.0 — CDX default CI alignment

Apply **over your existing Scout 1.2.0 repository**, including any indexing or dashboard hotfixes. These changes align seven stale regression-test assertions with the intentional CDX default of **1.5 seconds**, and add one test confirming that an explicit per-project override of **2.5 seconds** still works.

This overlay contains only five `tests/unit` files and this note. It changes **no application source**, networking, indexing, database, or acquisition code.

Run: `python scripts/run_tests.py --output validation/test-results`

The GitHub Actions run 38104349014 showed seven assertions expecting 2.5 while actual value is 1.5, repeated across eight jobs. Change the source default only if you prefer 2.5 globally; otherwise this test update is the appropriate correction.

CI on hosted Windows/macOS remains unverified until you push and rerun.
