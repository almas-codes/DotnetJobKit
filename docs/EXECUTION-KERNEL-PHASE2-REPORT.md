# Execution Kernel Phase 2 — Completion Report

Date: 2026-10-07  
Status: **INCOMPLETE** (see acceptance gate)

## 1. Implementation summary (this pass)

| Area | Change |
|------|--------|
| PostgreSQL pooling | `NpgsqlDataSource` + per-operation `OpenConnectionAsync` (no singleton live connection) |
| MySQL pooling | `MySqlDataSource` + per-operation connections; removed exhausted-lease dead UPDATE from hot claim loop |
| Batched `MaintainLeasesAsync` | Set-oriented PG (`unnest` + single UPDATE/RETURNING); MySQL (JOIN + UPDATE + SELECT in one tx); SQLite (temp table batch under writer gate) |
| Idempotency (schema) | PG partial unique index on active idempotency; MySQL unique `(idempotency_key, state)` |
| Control plane | Removed unused `ActiveLeaseTracker.cs` |
| Tests | `DeadlineEngineTests`, `ExecutionKernelCorrectnessTests`, `StoreConcurrentConnectionTests` (PG/MySQL skip without env) |
| Execution benchmark | New `tools/DotnetJobKit.ExecutionPerfRunner` — runs full host + `JobExecutionKernel` |

## 2. Architecture

See `docs/BENCHMARK-COMPARISON-100K.md` for before/after narrative. Phase 2 aligns stores with kernel expectations (pooling, batch maintenance, recovery off claim path for MySQL).

## 3. Requirement audit (updated)

| Requirement | Status |
|-------------|--------|
| PG `NpgsqlDataSource` | Done + concurrent test (env-gated) |
| MySQL pooled connections | Done + concurrent test (env-gated) |
| Single control plane | Done (kernel + deadline); no per-job renewal timers in runner |
| Real `DeadlineEngine` | Done + unit tests |
| Real batched maintenance | Done (PG/MySQL/SQLite) |
| Cancellation piggyback | Kernel uses maintenance results (existing integration tests) |
| Atomic `CommitOutcomeAsync` | Present all providers; adversarial continuation crash test **not added** |
| DB-enforced submit idempotency | Index added; **SubmitAsync still SELECT-then-INSERT on PG** (atomic upsert incomplete) |
| 50-way idempotency (DB) | In-memory only in new tests; **not provider integration** |
| `ClaimBatch` + `NextDueAt` | Still 2 round-trips (claim + MIN eligible) — documented |
| Runtime benchmark | Tool exists; **after** runs in progress; **before** baseline at `7f7b261` **not executed** |
| PostgreSQL EXPLAIN / scaling | **Not executed** |
| Full PART 35 matrix | **Partial** |

## 4. Correctness tests

| Suite | Result |
|-------|--------|
| `DotnetJobKit.Tests` | **8 passed**, 0 failed |
| `DotnetJobKit.IntegrationTests` (filtered) | **33 passed**, 0 failed |

New tests cover: deadline ordering/stale generation, stale `CommitOutcome`, duplicate commit idempotency, 50-way in-memory idempotency, runtime slot draining, PG/MySQL 32-way concurrent `GetAsync` (skip if no connection string).

## 5. 100K database benchmark (PerfRunner)

Unchanged methodology; prior results in `docs/BENCHMARK-COMPARISON-100K.md`:

| Provider | Claim-16 median before | after |
|----------|------------------------|-------|
| PostgreSQL | 608.7 ms | 560.5 ms |
| MySQL | 118.3 ms | 98.7 ms |
| SQLite | 51.4 ms | 50.2 ms |

**Not re-run in this pass** after Phase 2 store changes.

## 6. Execution kernel benchmark

Tool: `dotnet run -c Release --project tools/DotnetJobKit.ExecutionPerfRunner -- --jobs=100000 --iterations=3 --providers=sqlite|postgres|mysql`

Raw output directory: `docs/benchmarks/execution/after/`  
Smoke: 5,000 SQLite → ~1,782 jobs/sec, kernel confirmed in logs.

**Before** (`docs/benchmarks/execution/before/` at commit `7f7b261`): **not run** — requires worktree + same runner against pre-kernel coordinator.

Scenarios B–D (maintenance load, cancellation subset, continuation subset): **not implemented** in runner.

## 7. PostgreSQL claim analysis

**Not executed** (`EXPLAIN (ANALYZE, BUFFERS, WAL)` at 100K/1M/10M).

## 8. Performance table (PART 38)

| Provider | Scenario | Metric | Before | After | Notes |
|----------|----------|--------|--------|-------|-------|
| SQLite | 100K | claim latency | 51.4 ms | 50.2 ms | PerfRunner (prior pass) |
| PostgreSQL | 100K | claim latency | 608.7 ms | 560.5 ms | PerfRunner (prior pass) |
| MySQL | 100K | claim latency | 118.3 ms | 98.7 ms | PerfRunner (prior pass) |
| SQLite | execution | jobs/sec | — | see `after/*.json` | 100K×3 runs when complete |
| PostgreSQL | execution | jobs/sec | — | — | Not run |
| MySQL | execution | jobs/sec | — | — | Not run |

## 9. Acceptance gate (PART 39)

Mandatory items still **false**:

- [ ] Baseline runtime benchmark at `7f7b261`
- [ ] Full 100K×3 execution comparison all providers
- [ ] Scenarios B–D in execution benchmark
- [ ] PostgreSQL EXPLAIN / claim scaling study
- [ ] DB operation instrumentation in execution benchmark
- [ ] Provider 50-way idempotency integration test
- [ ] Continuation crash / adversarial test
- [ ] Batch maintenance SQL command-count tests (32 leases)
- [ ] Full PART 35 correctness matrix
- [ ] Re-run 100K PerfRunner after Phase 2 changes
- [ ] Atomic PG/MySQL/SQLite `SubmitAsync` with conflict handling

## 10. Files

| Kind | Path |
|------|------|
| Audit | `docs/EXECUTION-KERNEL-PHASE2-AUDIT.md` |
| Report | `docs/EXECUTION-KERNEL-PHASE2-REPORT.md` |
| 100K compare | `docs/BENCHMARK-COMPARISON-100K.md` |
| Execution raw | `docs/benchmarks/execution/after/` |
| Kernel | `src/DotnetJobKit/Execution/*` |
| Stores | `*JobStore.DataSource.cs`, `*JobStore.Execution.cs` |
| Runner | `tools/DotnetJobKit.ExecutionPerfRunner/` |

## 11. Conclusion

Phase 2 materially closes **connection lifecycle** and **batched lease maintenance** gaps and introduces a **real execution-kernel benchmark harness**, but mandatory **before/after runtime comparison**, **EXPLAIN**, **full scenario matrix**, and several **adversarial/idempotency** requirements remain. **Do not declare architecture complete.**
