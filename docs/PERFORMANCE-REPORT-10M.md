# DotnetJobKit performance report (10M)

Generated (UTC): `2026-10-06T22:02:32Z` (claim re-run; insert from earlier same session)

Job count: **10,000,000** (PostgreSQL, database `dotnetjobkit_perf`)

## Summary

| Phase | Result | Notes |
|---|---|---|
| Bulk insert (COPY) | **~23 min** (~7.2k jobs/s) | Completed in `docs/perf-10m-run-2.log`; first claim attempt failed with `Exception while reading from stream` (default command timeout / long-running connection). |
| Claim 16 jobs | **33.7 s** | Re-measured with `--claim-only` after `Command Timeout=0` on Npgsql. Table still held ~10M ready rows; index `ix_djk_jobs_dispatch` used. |
| Working set (claim run) | ~28 MB | Claim does not load full backlog into RAM. |

## Latest claim-only run

| Provider | OK | Insert time | Insert throughput | Claim 16 jobs | RAM delta (WS) | CPU (process ms) |
|---|---:|---:|---:|---:|---:|---:|
| postgres | yes | — | — | 33737.5ms | 0.0 MB | 781ms |

## How to reproduce

```powershell
cd "github projects/DotnetJobKit"
$env:DOTNETJOBKIT_PG_CONNECTION="Host=localhost;Port=5432;Username=postgres;Password=<secret>;Database=postgres"

# Full insert + claim (~25+ min insert on typical dev hardware):
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner -- --jobs=10000000 --providers=postgres

# Claim only (after rows already present):
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner -- --jobs=10000000 --providers=postgres --claim-only
```

Raw JSON: `PERFORMANCE-REPORT-10M.json`.

**MySQL:** not run at 10M (~1.4k/s insert would take many hours).
