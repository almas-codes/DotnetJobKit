# Architecture

## State machine

States: `Ready`, `Leased`, `Succeeded`, `Dead`, `Cancelled`.

`EligibleAt` meaning:

| State | EligibleAt |
|---|---|
| Ready | Earliest time the job may be claimed |
| Leased | Lease expiration (recovery threshold) |
| Terminal | `null` |

Claim eligibility:

```
(State = Ready OR (State = Leased AND EligibleAt <= now))
AND EligibleAt <= now
AND Queue in worker subscription
```

Expired lease with `AttemptCount >= MaxAttempts` → `Dead` during claim (never stuck in `Leased`).

## Ownership

V1 uses monotonic `AttemptCount` as the job-state version. Settlement requires matching `(JobId, AttemptCount, State = Leased)`.

`AttemptCount` fences **job row mutations only**, not external side effects.

## Runtime

Single `JobRuntimeCoordinator` (`BackgroundService`):

- Claims up to free capacity only
- Executes handlers on the thread pool (`ValueTask`, no default `Task.Run`)
- Renews leases via in-memory deadline tracking
- Sleeps until: next eligible job, next renewal, reconciliation, or wake signal
- Cooperative execution timeout via linked `CancellationToken`

## Provider boundary (`IJobStore`)

Atomic operations only: `Submit`, `Claim`, `Renew`, `Settle`, cancel paths, `GetNextEligibleAt`, terminal cleanup.

No generic repository layer.

## Retention

Default: `DeleteOnSuccess = true`. Failed/cancelled rows deleted in bounded batches during reconciliation.
