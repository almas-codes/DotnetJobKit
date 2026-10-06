# Performance

## Benchmarks (Phase 3)

Run the full matrix:

```powershell
cd "d:\add-astra-max\github projects\DotnetJobKit"
dotnet run -c Release --project benchmarks/DotnetJobKit.Benchmarks
```

Quick SQLite claim comparison (AttemptCount vs LeaseToken):

```powershell
dotnet run -c Release --project benchmarks/DotnetJobKit.Benchmarks -- --quick
```

### Baselines measured

| Benchmark | What it compares |
|---|---|
| `RuntimeComparisonBenchmarks` | `BackgroundService` + bounded `Channel<T>` vs DotnetJobKit in-memory |
| `SqliteClaimBenchmarks` | SQLite claim with `AttemptCount` fencing vs `LeaseToken` GUID |

Record results in this file after each release run.

## Stress / scale tests

| Variable | Meaning |
|---|---|
| `DOTNET_JOBKIT_MEGA_JOBS` | Backlog size for `MegaScaleTests` (default **1,000,000**) |
| `DOTNET_JOBKIT_RUN_10M=1` | Enables **10,000,000** job insert + runtime processing test |

```powershell
$env:DOTNET_JOBKIT_MEGA_JOBS = "10000000"
dotnet test tests/DotnetJobKit.IntegrationTests -c Release --filter "FullyQualifiedName~MegaScaleTests.Sqlite_large_backlog"

$env:DOTNET_JOBKIT_RUN_10M = "1"
dotnet test tests/DotnetJobKit.IntegrationTests -c Release --filter "FullyQualifiedName~Runtime_processes_batch"
```

Expect multi-minute runs and several GB disk for 10M rows.

## Design targets

- Claim bounded by `MaxConcurrency` (no full backlog in RAM)
- Idle coordinator sleeps on next eligible / renewal / signal
- Delete-on-success keeps hot table small
- `BEGIN IMMEDIATE` + WAL + tuned cache for SQLite writers
