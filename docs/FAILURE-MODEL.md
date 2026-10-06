# Failure model

## Delivery semantics

At-least-once execution. Handlers must be idempotent where external effects matter.

## Crash windows

| Crash point | Expected outcome |
|---|---|
| After claim, before handler | Lease expires → reclaim if attempts remain, else `Dead` |
| During handler | Same as failure; retry if attempts remain |
| During renewal | Lease may expire → recovery path |
| During settle | Re-execution possible if lease still valid or recovered |

## Stale owner

A worker that finished late cannot settle if `AttemptCount` no longer matches.

## Timeout

Default: `TimeoutBehavior.Cooperative` — request cancellation, **keep renewing/protecting lease** until the handler task completes, then settle.

## Cancellation

- `Ready` → `Cancelled` atomically
- `Leased` → `CancellationRequested = 1`; runtime cancels execution token and settles to `Cancelled`

## Notifications

`IJobWakeSignal` is a hint. Correctness does not depend on it; reconciliation and `GetNextEligibleAt` bound sleep (`MaxIdleSleep`).
