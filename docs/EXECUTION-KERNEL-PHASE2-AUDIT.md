# Execution Kernel Phase 2 — Working Tree Audit

Audit date: 2026-10-07 (pre–phase-2 completion pass).  
Scope: uncommitted kernel work on `main` vs Phase 2 specification.

| Requirement | Source implementation | Tests | Benchmark | Status |
|-------------|----------------------|-------|-----------|--------|
| DB connection lifecycle (PostgreSQL) | `PostgreSqlJobStore` holds one `NpgsqlConnection` for store lifetime | None | N/A | **C** — must use `NpgsqlDataSource` + per-op connections |
| DB connection lifecycle (MySQL) | `MySqlJobStore` holds one `MySqlConnection` | None | N/A | **C** — must use pooled `MySqlDataSource` |
| Single control plane | `JobExecutionKernel` + `DeadlineEngine`; coordinator timers removed; `ActiveLeaseTracker` orphaned | Partial (cancellation integration) | N/A | **B** — verify no duplicate timers; remove dead code |
| Deadline engine | `DeadlineEngine` (`PriorityQueue`, generation) | None dedicated | N/A | **B** — real structure; needs unit tests |
| Batched lease maintenance | Providers loop `RenewAsync` + `IsCancellationRequestedAsync` | None | N/A | **D** — fake batch; not spec-compliant |
| Cancellation piggyback | Kernel uses `MaintainLeasesAsync` results | `CancellationPollingTests` | N/A | **B** — works via store API; store must batch |
| Ownership epoch | `AttemptCount` + optional `LeaseToken` on mutations | Partial integration | N/A | **B** — needs stale-epoch matrix |
| Atomic CommitOutcome | PG/MySQL/SQLite/InMemory `CommitOutcomeAsync` in one transaction | Limited | N/A | **B** — pre-check outside tx races; needs adversarial tests |
| Retry-safe outcome idempotency | Outcome key on successor row / in-memory map | None | N/A | **B** — needs DB unique + duplicate commit test |
| Idempotency (submit) | SELECT then INSERT | Some enqueue tests | N/A | **C** — no DB unique / atomic upsert |
| Batch claim | `ClaimBatchAsync` = `ClaimAsync` + `GetNextEligibleAtAsync` | Existing claim tests | 100K claim-16 only | **B** — second query; not fused |
| NextDueAt hint | Second query after claim | None | N/A | **B** — documented gap for PG until fused |
| Recovery separation | Exhausted sweep removed from PG hot claim; `RecoverExhaustedLeasesBatchAsync` in retention | None | N/A | **B** — verify all providers |
| Retention separation | `RetentionMaintenanceService` | None dedicated | N/A | **B** — needs isolation test |
| Execution/admin split | Kernel vs retention service | N/A | N/A | **A** |
| Compiled handler invocation | `JobHandleInvokerFactory` at registration | N/A | N/A | **A** |
| Runtime benchmark | None (`PerfRunner` = insert + claim 16) | N/A | 100K DB only | **D** — missing execution kernel benchmark |
| Adversarial tests | None for continuation crash / 50-way idempotency | N/A | N/A | **D** |

## Component notes

- **JobExecutionKernel** (`src/DotnetJobKit/Execution/JobExecutionKernel.cs`): claim loop, slots, deadlines, `CommitOutcomeAsync`, batched maintenance calls.
- **HandlerExecutionRunner**: invoke-only (no per-job timers).
- **PostgreSqlJobStore.Execution.cs**: `MaintainLeasesAsync` is per-row renew + cancel read.
- **100K benchmark**: `docs/BENCHMARK-COMPARISON-100K.md`, raw under `docs/benchmarks/100k/` — does not exercise runtime kernel.

## Phase 2 exit criteria

This audit must be updated in the final report when each row reaches **A** with tests/benchmarks as required.
