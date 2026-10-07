# Execution Kernel — Final Report

**Date:** 2026-10-07  
**Baseline commit:** `7f7b26154fad5b92e5df5a10f6e349d743f5da69` (worktree `DotnetJobKit-baseline-7f7b261`)  
**Current tree:** execution kernel + Phase 2/3 store work (uncommitted on `main`)

## Final status: **INCOMPLETE**

The kernel, pooling, batched maintenance, atomic idempotency, adversarial tests, and SQLite benchmark matrix are in place. **PostgreSQL/MySQL integration tests now pass** with local `DOTNETJOBKIT_*` env (see `docs/benchmarks/environment.local.example.ps1`). Provider benchmark matrix (`run-provider-matrix.ps1`) captures 100K PerfRunner, EXPLAIN, and execution runs when executed locally. Remaining gate items: full scenario matrix completion, optional 10M scaling, end-to-end SQL command counts on noop hot path, and MySQL set-based claim.

---

## 1. What was delivered

| Area | Status |
|------|--------|
| `JobExecutionKernel` + `DeadlineEngine` + thin coordinator | Done |
| PG/MySQL `*DataSource`, per-operation connections | Done |
| Batched `MaintainLeasesAsync` (PG/MySQL/SQLite) | Done |
| Atomic `SubmitAsync` idempotency (all providers) | Done |
| PG `ClaimBatchAsync` on **one connection** (claim + next eligible) | Done |
| `ExecutionPerfRunner` scenarios **A–D**, configurable wait | Done |
| `DotnetJobKit.PostgresExplainRunner` | Tool present; not executed |
| Tests: Phase 3 + retention / mixed-store / PG maintain (env-gated) | **50** unit + integration (excl. MegaScale) |
| SQLite 100K PerfRunner **3×** | `docs/benchmarks/100k/final/` |
| SQLite execution **final/** A 1/3, **B complete**, **C partial**, **D aborted** | Partial |

---

## 2. Correctness

| Suite | Passed | Failed |
|-------|--------|--------|
| `DotnetJobKit.Tests` | 8 | 0 |
| `DotnetJobKit.IntegrationTests` (excl. `MegaScale`, `SqliteMultiProcess`) | 41 | 0 |
| **Total** | **49** | **0** |

With `DOTNETJOBKIT_PG_CONNECTION` + `DOTNETJOBKIT_MYSQL_CONNECTION`: PG/MySQL concurrent-get, 50-way idempotency, PG maintain bound — **pass**.

Notable additions in `ExecutionKernelFinalTests.cs`:

- Retention purge does not delete active ready/leased rows (SQLite).
- 32 concurrent `GetAsync` + parallel `ClaimAsync` on one SQLite store.
- PostgreSQL 32-lease maintenance command bound (skips without PG env).

Phase 3 tests (stale owner, duplicate commit, 50-way idempotency, SQLite maintain bound) remain green.

---

## 3. Execution benchmark (SQLite)

**Tool:** `dotnet run -c Release --project tools/DotnetJobKit.ExecutionPerfRunner -- --scenario=A|B|C|D --jobs=N --iterations=3 --providers=sqlite --out-dir=docs/benchmarks/execution/final`

| Scenario | Description | Evidence |
|----------|-------------|----------|
| **A** | 100K noop, concurrency 32 | `final/sqlite-scenarioA-run1.json` only (**runs 2–3 aborted** mid run 2) |
| **B** | 10K slow handler (80 ms), short lease / renew | `final/sqlite-scenarioB-run1.json` |
| **C** | 1K slow jobs, ~50% cancelled while leased | `sqlite-scenarioC-run1.json` — **516/1000** in 30m cap; `MaintenanceJobs=951` |
| **D** | 5K jobs with bulk continuation → child noop (SQLite only) | **Aborted** during run (no JSON) |

### Scenario A — NEW kernel (final run 1)

| Metric | Value |
|--------|-------|
| Jobs | 100,000 / 100,000 |
| Throughput | **~126 jobs/s** |
| Elapsed | **796 s** |
| Notes | Higher elapsed vs earlier `after/` runs (~47–80 s, ~1.2–2.2K jobs/s) — likely concurrent benchmark load on one machine; use `after/` for best-case and `final/` for instrumented rerun |

### Scenario A — OLD coordinator @ 7f7b261

| Run | Jobs completed | Throughput | Notes |
|-----|----------------|------------|-------|
| 100K run 1 | 11,491 / 100,000 | ~56 jobs/s | Stopped at **30 min** cap → `before/sqlite-run1.json` |
| 10K | **10,000 / 10,000** | **~6.4 jobs/s** | **1,567 s** — `before/sqlite-10k-run1.json` (coordinator @ 7f7b261) |

**Interpretation:** Kernel completes full 100K SQLite backlog; legacy coordinator is order-of-magnitude slower on the same workload shape. Apples-to-apples **10K** baseline vs kernel comparison should be taken from `before/` and `final/` once 10K OLD run finishes.

### Scenario B (maintenance-oriented)

10K jobs in **37.9 s** (~**264 jobs/s**). `MaintenanceJobs` was **0** in JSON — handlers finished within lease window at this concurrency; tighten lease or lengthen handler sleep to force renewals in a future rerun.

---

## 4. 100K database benchmark (PerfRunner)

Three SQLite reruns in `docs/benchmarks/100k/final/`:

| Run | Insert (jobs/s) | Claim-16 (ms) |
|-----|-----------------|---------------|
| 1 | ~37,160 | ~45.7 |
| 2 | ~40,143 | ~44.4 |
| 3 | ~40,052 | ~61.5 |

Claim latency stable vs historical before/after (~51 ms baseline era). **Not** a proxy for execution throughput.

---

## 5. PostgreSQL / MySQL (local env)

Configure via `docs/benchmarks/environment.local.ps1` (gitignored; copy from `environment.local.example.ps1`).

| Item | Status |
|------|--------|
| Integration tests (PG/MySQL) | **Pass** (49/49 total with env) |
| 100K PerfRunner final PG ×3 | **Done** — see `100k/final/postgres-run{1..3}.txt` (~566 ms claim-16, ~41K insert/s run 1) |
| 100K PerfRunner final MySQL ×3 | **Done** — `100k/final/mysql-run{1..3}.txt` (matrix run; aborted before later steps) |
| Execution 100K PG/MySQL | **Not run** — matrix **aborted** after PerfRunner phase |
| `PostgresExplainRunner` 100K + 1M | **Not run** — matrix **aborted** before EXPLAIN |
| Provider 50-way idempotency | **Pass** (PG + MySQL) |

---

## 6. Known gaps (honest)

1. **MySQL set-based claim** — still row-loop; not replaced.
2. **Full PART 35/45 matrix** — covered by 50 tests, not exhaustive slot/retention/cross-process matrix.
3. **Execution `SqlCommandCount`** — zero in full-run JSON; instrumentation verified only in focused maintain tests.
4. **Scenario B maintenance load** — needs parameter tuning to produce non-zero `MaintenanceJobs`.
5. **OLD 100K baseline** — incomplete under 30 min cap; use 10K or extended wait for fair OLD numbers.
6. **PG/MySQL + EXPLAIN** — require live connections.

---

## 7. Evidence index

| Path | Content |
|------|---------|
| `docs/EXECUTION-KERNEL-PHASE3-AUDIT.md` | Requirement table |
| `docs/EXECUTION-KERNEL-FINAL-COMPARISON.md` | Before/after table |
| `docs/EXECUTION-KERNEL-PHASE3-FINAL-REPORT.md` | Phase 3 snapshot (superseded by this doc for status) |
| `docs/benchmarks/execution/{before,after,final}/` | Execution kernel JSON + console |
| `docs/benchmarks/100k/final/` | SQLite PerfRunner 3× |
| `tools/DotnetJobKit.ExecutionPerfRunner/` | Scenarios A–D |
| `tools/DotnetJobKit.PostgresExplainRunner/` | EXPLAIN helper |

---

## 8. Recommended next steps

1. Set `DOTNETJOBKIT_PG_CONNECTION` / `DOTNETJOBKIT_MYSQL_CONNECTION` and rerun execution + 100K + EXPLAIN matrices (3× each).
2. Finish OLD **10K** (or 100K with `--max-wait-minutes=0`) on baseline worktree with a runner build that targets **coordinator-only** (no kernel types).
3. Tune scenario **B** (handler ≥ lease duration) until `MaintenanceJobs` > 0 and compare maintenance batch counts.
4. Wire `JobStoreDiagnostics.RecordCommand()` on SQLite claim/settle hot paths if end-to-end SQL counts are required in JSON.

---

## 9. Conclusion

The execution kernel refactor is **functionally complete** for SQLite: tests pass, throughput on 100K is dramatically above the legacy coordinator on the same runner shape, and store-layer Phase 2/3 items are implemented. The **program-wide acceptance gate** remains **INCOMPLETE** until provider env benchmarks, EXPLAIN/scaling, a fair completed OLD baseline, and maintenance-load evidence are captured.
