# SQLite provider

V1 durable store for single-node and multi-process experiments.

## Schema

Table `Jobs` with partial index `IX_Jobs_Dispatch` on `(Queue, EligibleAt, JobId)` where `State IN (0,1)`.

## PRAGMAs

- `journal_mode=WAL` (configurable)
- `busy_timeout` (default 5000 ms)

## Limitations

- Not recommended for large distributed production (use future SQL Server/PostgreSQL providers with the same `IJobStore` contract).
- Cross-process: no durable notify; rely on wake signal in-process + bounded reconciliation + `GetNextEligibleAt`.

## Claim

Short transaction: select one eligible row, conditional update with expected `AttemptCount`, increment attempt, set `Leased` and lease expiry.

Transactional enqueue with caller EF transaction is **not** implemented in V1 for SQLite; requires shared connection — documented as an extension point.
