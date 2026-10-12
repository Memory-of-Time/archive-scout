# Scout 1.2.1 — Local validation

- Full local suite after consolidated CDX recovery changes: **263 tests passed, 0 failures**, 6 skipped on headless Linux (from 263 executed).
- Focused restoration test suite: 8 passed (operations, lock, import, move, WAL, filtering, optional connection recovery).
- Responsive Media page layout: passed in Xvfb at 940×680 and 1280×720.
- Native Tk GUI smoke: passed in isolated virtual-display process.
- Version/build metadata: `python scripts/verify_release.py` passed.
- Healthy 90-save localhost benchmark: original 1.2.0: 7.919 acquisition saves/s; candidate: 8.000 acquisition saves/s; 90/90 SHA256 verified in each run. Values reflect run-to-run jitter, not proven speed improvement.
- Replay delay: 0.125s and workers: 10 unchanged.
- Live Wayback sustained throughput and hosted CI: **not tested**.

No performance change should be accepted as final until representative multi-hour and cross-platform runs are compared.

## Consolidated CDX outage-recovery verification

- New `test_v121_cdx_outage_recovery.py` covers HTTP 408/429/500/502/503/504 and backend connection failures, response-size splits, narrow timeout protections, an existing one-second checkpoint, no page-size shrink or queue rotation during an outage, and informative endpoint messages.
- The original `test_index_recovery.py` and `test_timeout_recovery.py` continue to pass, including broad-query subdivision and durable stop/resume.
- Automatic Timemap strategy remains optional. Test expectations work both with the base v1.2.0 automatic-resume strategy and with the separate Detective Ra automatic-paged add-on.
- Clean v1.2.0 + consolidated patch: **261 tests, 0 failures, 6 headless skips**; optional Ra Timemap + consolidated patch: **263 tests, 0 failures, 6 skips**. Both trees passed `scripts/verify_release.py`.
- Matching 90-capture localhost benchmark with SHA-256 verification: v1.2.0 baseline **8.040 acquisition saves/s** vs consolidated v1.2.1 **8.042 acquisition saves/s** (run-to-run jitter, not a speed improvement). No measured healthy acquisition regression in these short controlled runs.
- Live Wayback indexing improvement, long-run behavior, and hosted cross-platform CI have **not** been measured. No positive live performance claims should be inferred from an unchanged healthy request limiter.
