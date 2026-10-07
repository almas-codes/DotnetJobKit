# DotnetJobKit 100K benchmark comparison (execution kernel)

Baseline commit (code unchanged during baseline capture): `7f7b26154fad5b92e5df5a10f6e349d743f5da69`

After implementation: working tree on same branch (not committed per task instruction).

Environment details: `docs/benchmarks/100k/before/environment.txt`

Raw outputs:

- Before: `docs/benchmarks/100k/before/`
- After: `docs/benchmarks/100k/after/`

## Benchmark command (identical before/after)

```powershell
cd "github projects/DotnetJobKit"
dotnet build -c Release tools/DotnetJobKit.PerfRunner/DotnetJobKit.PerfRunner.csproj
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner --no-build -- --jobs=100000 --providers=sqlite
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner --no-build -- --jobs=100000 --providers=postgres
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner --no-build -- --jobs=100000 --providers=mysql
```

PerfRunner measures **bulk insert of N ready jobs** plus **one `ClaimAsync` batch of 16** jobs. It does **not** run `JobRuntimeCoordinator` / execution kernel, lease maintenance, or handler execution.

## Current architecture (baseline)

- **Claim**: `IJobStore.ClaimAsync` per provider; coordinator loop calls claim up to `MaxConcurrency - activeExecutions`.
- **Execute**: `JobRuntimeCoordinator` fire-and-forget `ExecuteClaimedJobAsync` → `HandlerExecutionRunner` with **per-job** lease renewal timer and cancellation polling timers.
- **Renew / cancel**: `RenewAsync` + `IsCancellationRequestedAsync` from runner timers; **plus** `ProcessRenewalsAsync` on coordinator via `ActiveLeaseTracker`.
- **Settle**: `SettleAsync` / `CompleteAsRecurringReadyAsync` from coordinator after handler; **continuation** via separate `SubmitAsync`.
- **Retention**: `DeleteTerminalBatchAsync` inside coordinator loop on `ReconciliationInterval`.
- **Idle wait**: `JobWakeSignal.WaitUntilAsync` + **250ms wait** when jobs active; `GetNextEligibleAtAsync` second round-trip when idle.
- **PostgreSQL claim**: set-oriented CTE update; **exhausted-lease dead sweep** ran in the **same transaction** before each claim batch.
- **Idempotency**: check-then-insert (no DB unique enforcement on `idempotency_key` in baseline schema).

## Target architecture (after — implemented)

- **Single control plane**: `JobExecutionKernel` + `DeadlineEngine` (`PriorityQueue` with generation stamps).
- **Handler path**: `HandlerExecutionRunner.ExecuteAsync` only invokes compiled `JobHandleInvoker` (reflection once at registration).
- **Maintenance**: `MaintainLeasesAsync` (combined renew + cancellation read); scheduled via deadline engine at `min(LeaseRenewInterval, CancellationPollInterval)`.
- **Outcome**: `CommitOutcomeAsync` (parent transition + optional successor in one transaction where implemented).
- **Claim**: `ClaimBatchAsync` returns claimed jobs + `NextDueAt` hint (implemented; PG/MySQL/SQLite currently delegate claim + separate next-eligible query except PG dead sweep removed from hot path).
- **Recovery / retention**: `RecoverExhaustedLeasesBatchAsync` + `DeleteTerminalBatchAsync` in `RetentionMaintenanceService` (off hot dispatch path).
- **Ownership**: `JobId` + `AttemptCount` (+ optional `LeaseToken`) on all mutations; stale commits return `StaleOwner`.

## Baseline measured runs (3× per provider)

| Provider | Insert time (s) | Insert jobs/s | Claim 16 (ms) | Claimed count |
|---|---:|---:|---:|---:|
| SQLite run-1 | 2.4 | 42,441 | 44.3 | 16 |
| SQLite run-2 | 2.4 | 41,917 | 51.4 | 16 |
| SQLite run-3 | 2.2 | 46,012 | 58.1 | 16 |
| Postgres run-1 | 2.4 | 42,032 | 602.1 | 16 |
| Postgres run-2 | 2.5 | 39,539 | 665.5 | 16 |
| Postgres run-3 | 2.4 | 41,981 | 608.7 | 16 |
| MySQL run-1 | 68.1 | 1,468 | 118.3 | 16 |
| MySQL run-2 | 76.6 | 1,305 | 116.7 | 16 |
| MySQL run-3 | 67.4 | 1,484 | 119.9 | 16 |

### Baseline aggregates (3 runs)

| Provider | Insert median (s) | Insert avg jobs/s | Claim 16 median (ms) |
|---|---:|---:|---:|
| SQLite | 2.4 | 42,441 | 51.4 |
| PostgreSQL | 2.4 | 41,981 | 608.7 |
| MySQL | 68.1 | 1,468 | 118.3 |

## After measured runs (3× per provider)

| Provider | Insert time (s) | Insert jobs/s | Claim 16 (ms) | Claimed count |
|---|---:|---:|---:|---:|
| SQLite run-1 | 2.3 | 42,558 | 50.2 | 16 |
| SQLite run-2 | 2.4 | 41,064 | 41.8 | 16 |
| SQLite run-3 | 2.4 | 42,154 | 56.7 | 16 |
| Postgres run-1 | 2.5 | 40,092 | 560.5 | 16 |
| Postgres run-2 | 2.5 | 39,564 | 528.0 | 16 |
| Postgres run-3 | 2.8 | 36,236 | 566.6 | 16 |
| MySQL run-1 | 66.5 | 1,504 | 117.6 | 16 |
| MySQL run-2 | 66.2 | 1,510 | 98.7 | 16 |
| MySQL run-3 | 66.4 | 1,506 | 95.6 | 16 |

### After aggregates (3 runs)

| Provider | Insert median (s) | Insert avg jobs/s | Claim 16 median (ms) |
|---|---:|---:|---:|
| SQLite | 2.4 | 42,154 | 50.2 |
| PostgreSQL | 2.5 | 39,564 | 560.5 |
| MySQL | 66.4 | 1,506 | 98.7 |

## Before vs after

Latency improvement % = `(Before − After) / Before × 100` (positive = faster).

Throughput improvement % = `(After − Before) / Before × 100`.

| Provider | Metric | Before (median) | After (median) | Abs change | % change | Result |
|---|---|---:|---:|---:|---:|---|
| SQLite | Insert time (s) | 2.4 | 2.4 | 0.0 | 0% | UNCHANGED |
| SQLite | Insert throughput (jobs/s) | 42,441 | 42,154 | −287 | −0.7% | UNCHANGED |
| SQLite | Claim 16 (ms) | 51.4 | 50.2 | −1.2 | +2.3% | UNCHANGED |
| PostgreSQL | Insert time (s) | 2.4 | 2.5 | +0.1 | −4.2% | UNCHANGED |
| PostgreSQL | Insert throughput (jobs/s) | 41,981 | 39,564 | −2,417 | −5.8% | UNCHANGED |
| PostgreSQL | Claim 16 (ms) | 608.7 | 560.5 | −48.2 | **+7.9%** | IMPROVED |
| MySQL | Insert time (s) | 68.1 | 66.4 | −1.7 | +2.5% | UNCHANGED |
| MySQL | Insert throughput (jobs/s) | 1,468 | 1,506 | +38 | +2.6% | UNCHANGED |
| MySQL | Claim 16 (ms) | 118.3 | 98.7 | −19.6 | **+16.6%** | IMPROVED |

Working set (PerfRunner console): ~28 MB SQLite, ~41–42 MB PostgreSQL, ~43–44 MB MySQL — **NOT MEASURED** as controlled delta before/after.

CPU process time, allocations, SQL command counts, runtime scheduler wakeups, lease maintenance counts: **NOT MEASURED** by PerfRunner.

## Correctness before vs after

| Area | Before | After |
|---|---|---|
| Unit tests | 4 passed | 4 passed |
| Integration tests (filtered) | 27 passed | 27 passed |
| Raw logs | `docs/benchmarks/100k/before/tests.txt` | `docs/benchmarks/100k/after/tests.txt` |

Dedicated adverse failure / stale-owner / concurrent idempotency tests from spec §41: **NOT IMPLEMENTED** in this pass.

## What improved (measured)

1. **PostgreSQL claim-16 median** 608.7 ms → 560.5 ms (~7.9% faster). Likely aligns with removing the **exhausted-lease UPDATE sweep** from the normal claim transaction (recovery moved to `RecoverExhaustedLeasesBatchAsync`).
2. **MySQL claim-16 median** 118.3 ms → 98.7 ms (~16.6% faster). Same PerfRunner path; no claim SQL rewrite in this pass — treat as **environment variance** unless reproduced on more runs.

## What did not improve

- SQLite insert and claim medians within noise.
- PostgreSQL insert throughput median slightly lower (run-3 insert outlier at 2.8s).
- PerfRunner does not exercise execution kernel, batched lease maintenance, or `CommitOutcomeAsync` hot paths.

## Regressions

- None beyond normal run-to-run variance on insert throughput for PostgreSQL (see raw files).

## Remaining bottlenecks (from PerfRunner scope)

- **MySQL bulk insert** ~66s @ ~1.5k jobs/s (batch size 2,000 in tool).
- **PostgreSQL claim** ~0.53–0.61s for 16 rows at 100k backlog (not full table scan at this scale; EXPLAIN not captured in this pass).

## Final decision

**ARCHITECTURE RESULT: MIXED**

1. Measurably faster on **claim** for PostgreSQL (and possibly MySQL) at 100k rows under PerfRunner — **yes, modest**.
2. SQLite claim/insert: **no material change**.
3. PostgreSQL insert: **unchanged to slightly noisy**.
4. MySQL insert: **slight improvement** (within variance).
5. Runtime scheduler / lease / handler paths: **not benchmarked** by this harness.
6. Correctness: **no regression** in existing tests.
7. Largest measured benefit: **PostgreSQL claim path** after removing in-claim exhausted-lease sweep.
8. No benefit proven for **SQLite** or **insert** workloads in this comparison.
9. Next bottlenecks to address: **MySQL insert batching**, **PostgreSQL claim at multi-million row** (optional 1M diagnostic), **set-oriented `MaintainLeasesAsync` on PG**, **NpgsqlDataSource** lifecycle, **DB-enforced idempotency**, **adverse ownership tests**.

## Not completed from specification

- `EXPLAIN (ANALYZE, BUFFERS)` on production claim SQL
- Full PostgreSQL/MySQL set-oriented `MaintainLeasesAsync` UPDATE…RETURNING
- MySQL claim batch rewrite (still per-row loop in store)
- `NpgsqlDataSource` singleton refactor (still long-lived `NpgsqlConnection` in `PostgreSqlJobStore`)
- Unique index + transactional idempotency race tests
- Optional 1M diagnostic benchmark
- Git commits (explicitly skipped per user request)
