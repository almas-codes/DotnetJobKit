# Execution Kernel Phase 3 — Final Report

## 1. Final Status

**INCOMPLETE** — see **`docs/EXECUTION-KERNEL-FINAL-REPORT.md`** for the consolidated gate, benchmarks, and evidence index.

Completion gate items still missing evidence: full baseline execution JSON, scenarios B–D, PG/MySQL execution + 100K final matrices (3× each), PostgreSQL EXPLAIN/scaling (requires `DOTNETJOBKIT_PG_CONNECTION`), fused ClaimBatch/NextDueAt, full PART 45 matrix, retention isolation test, shared-store mixed-operation concurrency test.

## 2. Implementation Completed (verified)

- **Atomic idempotency submit**: PostgreSQL (`ON CONFLICT` + expired-key clear in tx), MySQL (duplicate-key + tx), SQLite (`ON CONFLICT` partial index + tx)
- **Active idempotency indexes**: PG partial unique; SQLite partial unique; MySQL `(idempotency_key, state)` + runtime index ensure
- **Batch lease maintenance instrumentation**: `JobStoreDiagnostics.SqlCommandCount`; SQLite bulk temp-table path (4 commands for 32 leases)
- **Kernel/store diagnostics**: `ExecutionKernelDiagnostics` (deadlines, maintenance batches, scheduler wakeups)
- **Phase 3 tests**: stale reclaim, duplicate commit replay, 50-way SQLite idempotency, 32-lease command bound, provider idempotency (env-gated)
- **Tools**: `DotnetJobKit.PostgresExplainRunner`, enhanced `DotnetJobKit.ExecutionPerfRunner` metrics JSON
- **Baseline worktree**: `DotnetJobKit-baseline-7f7b261` + copied ExecutionPerfRunner (benchmark-only)
- **100K PerfRunner rerun (SQLite)**: claim 52.7 ms, insert 43,498 jobs/s → `docs/benchmarks/100k/final/sqlite-run1.txt`

## 3. Requirement Audit

See `docs/EXECUTION-KERNEL-PHASE3-AUDIT.md`.

## 4–5. Architecture Before/After

Unchanged narrative from Phase 2: before = coordinator + per-job timers + `ActiveLeaseTracker`; after = `JobExecutionKernel` + `DeadlineEngine` + batched `MaintainLeasesAsync` + `CommitOutcomeAsync`.

## 6. Correctness Results

| Suite | Passed | Failed |
|-------|--------|--------|
| `DotnetJobKit.Tests` | 8 | 0 |
| `DotnetJobKit.IntegrationTests` (filtered) | 39 | 0 |
| **Total** | **47** | **0** |

New Phase 3 tests: 6/6 pass. Provider idempotency/concurrency tests skip without DB env vars.

## 7. 100K Database Benchmark

| | Historical before (7f7b261) | Phase 2 after | **Final rerun (SQLite)** |
|--|--|--|--|
| Claim-16 | 51.4 ms | 50.2 ms | **52.7 ms** |

PG/MySQL final reruns not executed (env).

## 8. Execution Kernel Benchmark

**NEW (after/, 100K SQLite, kernel):**

| Run | jobs/sec | Elapsed (s) |
|-----|----------|-------------|
| 1 | 1,241 | 80.6 |
| 2 | 2,102 | 47.6 |
| 3 | 2,169 | 46.1 |

**OLD (before/, 100K SQLite, baseline coordinator @ 7f7b261):** `sqlite-run1.json` — **11,491** jobs in **1,800 s** (~**56 jobs/s**); PerfRunner **30-minute wait cap** stopped the host before 100K completed. **Not a full 100K baseline** until deadline is raised and rerun.

## 9–10. PostgreSQL EXPLAIN / Scaling

Tool: `tools/DotnetJobKit.PostgresExplainRunner`. **Not executed** in this environment without PostgreSQL connection.

## 11. DB Operation Counts

Execution runner JSON (new builds) includes `SqlCommandCount`, `MaintenanceBatches`, `MaintenanceJobs`. Historical `after/*.json` predate fields; re-run `final/` pending.

## 12. CPU / Memory / Allocations

Execution JSON includes RSS before/after, CPU ms; extended fields include GC collection counts on new runner builds.

## 13–16. What Improved / Did Not / Regressions / Bottlenecks

- **Improved (code-level)**: pooled PG/MySQL connections, set-oriented maintenance, atomic idempotency, kernel diagnostics
- **Not measured end-to-end yet**: maintenance DB ops/job old vs new (baseline run incomplete)
- **Regressions**: none in test suite
- **Bottlenecks**: SQLite single-writer; old baseline runtime on 100K expected slower (timer overhead)

## 17. Recommended Next Optimization

1. Finish baseline 100K execution capture and scenarios B–D on both runtimes  
2. Fuse PostgreSQL `ClaimBatch` + `NextDueAt` in one transaction  
3. MySQL set-oriented claim (replace per-row loop)  
4. Run EXPLAIN at 100K/1M with live PostgreSQL  

## 18. Evidence Paths

- Audit: `docs/EXECUTION-KERNEL-PHASE3-AUDIT.md`
- Comparison: `docs/EXECUTION-KERNEL-FINAL-COMPARISON.md`
- Execution: `docs/benchmarks/execution/{before,after,final}/`
- 100K: `docs/benchmarks/100k/{before,after,final}/`
- EXPLAIN: `docs/benchmarks/postgres-explain/` (when run)
