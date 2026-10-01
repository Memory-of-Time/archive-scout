# Offline benchmarks

`scripts/benchmark_offline.py` exercises the main high-volume local paths without contacting the Internet Archive.

The benchmark can generate synthetic CDX rows, insert captures into a temporary project database, repeat the same rows to verify no-op indexing, exercise paginated result access, generate large literal keyword sets, and measure scoring behavior.

The benchmark is intended for regression comparison on the same machine and Python build. It is not a promise of universal throughput because storage, CPU, operating system, SQLite build, project shape, keyword rules, and Wayback response latency vary substantially.

Release validation includes a small benchmark smoke test in GitHub Actions. Larger local profiles are useful before changes to the parser, automaton, SQLite repository layer, or queue scheduling.
