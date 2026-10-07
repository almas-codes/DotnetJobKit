# Execution Kernel — Before/After Comparison

Baseline: worktree @ `7f7b261` (`JobRuntimeCoordinator` + per-job timers).  
New: current tree (`JobExecutionKernel` + `DeadlineEngine`).

**Authoritative status:** `docs/EXECUTION-KERNEL-FINAL-REPORT.md`

| Provider | Scenario | Metric | OLD | NEW | Change | Result |
|----------|----------|--------|-----|-----|--------|--------|
| SQLite | Execution A 10K (full OLD) | jobs/sec | **~6.4** (`before/sqlite-10k-run1.json`) | — | Run NEW 10K for pair | **PASS** OLD |
| SQLite | Execution A 10K | elapsed (s) | **1,567** (complete) | — | — | **PASS** OLD |
| SQLite | Execution A 100K | jobs completed | 11,491 / 100K (`sqlite-100k-partial-run1.json`) | 100,000 | Full drain | **PARTIAL** OLD |
| SQLite | Execution A 100K | jobs/sec | ~56 (`before/run1`) | ~126–2,169 (`final/run1` vs `after/`) | Large kernel gain | **PASS** (kernel) |
| SQLite | Execution A 100K | elapsed (s) | 1,800 (cap) | 47–796 | — | See report |
| SQLite | Execution B 10K | jobs/sec | — | ~264 | — | `final/sqlite-scenarioB-run1.json` |
| SQLite | 100K DB claim-16 | ms | ~51.4 (historical) | 44–62 (`100k/final` 3×) | Stable | **PASS** |
| SQLite | 100K DB insert | jobs/s | — | ~37–40K | — | **PASS** |
| PostgreSQL | Execution / 100K final | — | — | — | — | **SKIP** (no env) |
| MySQL | Execution / 100K final | — | — | — | — | **SKIP** (no env) |

Raw paths:

- `docs/benchmarks/execution/before/` — baseline coordinator  
- `docs/benchmarks/execution/after/` — early kernel runs (best throughput)  
- `docs/benchmarks/execution/final/` — instrumented scenarios A–D  
- `docs/benchmarks/100k/final/` — PerfRunner SQLite 3×  

Do not use DB claim-16 alone as proof of execution throughput.
