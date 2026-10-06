# DotnetJobKit performance report

Generated (UTC): `2026-10-06T22:03:00Z` (PostgreSQL row refreshed; SQLite/MySQL from prior full run same day)

Job count per provider test: **1,000,000**

## Summary

| Provider | OK | Insert time | Insert throughput | Claim 16 jobs | RAM delta (WS) | CPU (process ms) |
|---|---:|---:|---:|---:|---:|---:|
| sqlite | yes | 72.3s | 13,835/s | 2309.3ms | 0.0 MB | 68594ms |
| postgres | yes | 17.4s | 57,332/s | 1335.3ms | 0.0 MB | see JSON |
| mysql | yes | 732.7s | 1,365/s | 104.9ms | 0.0 MB | 309828ms |

PostgreSQL **claim** dropped from ~45s to ~1.3s after batched `SKIP LOCKED` claim and unlimited command timeout. Re-run `sqlite,mysql` with PerfRunner to refresh those rows on your machine.

## How to reproduce

```powershell
cd "github projects/DotnetJobKit"
$env:DOTNETJOBKIT_PG_CONNECTION="Host=localhost;Port=5432;Username=postgres;Password=<secret>;Database=postgres"
$env:DOTNETJOBKIT_MYSQL_CONNECTION="Server=localhost;Port=3306;Database=Adastra_QA;User=admin;Password=<secret>"
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner -- --jobs=1000000 --providers=sqlite,postgres,mysql
# 10 million (PostgreSQL):
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner -- --jobs=10000000 --providers=postgres
```

Raw JSON snapshot is written beside this file as `PERFORMANCE-REPORT.json`.

Ten-million results: [PERFORMANCE-REPORT-10M.md](PERFORMANCE-REPORT-10M.md).
