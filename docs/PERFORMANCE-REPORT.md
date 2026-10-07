# DotnetJobKit performance report

Generated (UTC): `2026-10-07T11:46:58.0399446Z`
Job count per provider test: **100,000**

## Summary

| Provider | OK | Insert time | Insert throughput | Claim 16 jobs | RAM delta (WS) | CPU (process ms) |
|---|---:|---:|---:|---:|---:|---:|
mysql | yes | 77.1s | 1,297/s | 123.5ms | 0.0 MB | 32359ms

## How to reproduce

```powershell
cd "github projects/DotnetJobKit"
$env:DOTNETJOBKIT_PG_CONNECTION="Host=localhost;Port=5432;Username=postgres;Password=<secret>;Database=postgres"
$env:DOTNETJOBKIT_MYSQL_CONNECTION="Server=localhost;Port=3306;Database=Adastra_QA;User=admin;Password=<secret>"
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner -- --jobs 1000000 --providers sqlite,postgres,mysql
# 10 million:
dotnet run -c Release --project tools/DotnetJobKit.PerfRunner -- --jobs 10000000 --providers postgres
```

Raw JSON snapshot is written beside this file as `PERFORMANCE-REPORT.json`.
