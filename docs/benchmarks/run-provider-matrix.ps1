$ErrorActionPreference = "Continue"
$localEnv = Join-Path $PSScriptRoot "environment.local.ps1"
if (-not (Test-Path $localEnv)) { throw "Create $localEnv from environment.local.example.ps1" }
. $localEnv
Set-Location (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent)

$log = Join-Path $PSScriptRoot "provider-matrix-console.txt"
function Log($m) { $line = "[$(Get-Date -Format o)] $m"; Add-Content $log $line; Write-Host $line }

Log "=== 100K PerfRunner postgres/mysql x3 ==="
foreach ($p in @("postgres","mysql")) {
  1..3 | ForEach-Object {
    Log "PerfRunner $p run $_"
    dotnet run -c Release --project tools/DotnetJobKit.PerfRunner --no-build -- --jobs=100000 --providers=$p 2>&1 | Out-File -Encoding utf8 (Join-Path $PSScriptRoot "100k/final/$p-run$_.txt")
  }
}

Log "=== EXPLAIN 100K + 1M ==="
dotnet run -c Release --project tools/DotnetJobKit.PostgresExplainRunner --no-build -- --jobs=100000 2>&1 | Out-File -Encoding utf8 (Join-Path $PSScriptRoot "postgres-explain/run-console-100k.txt")
dotnet run -c Release --project tools/DotnetJobKit.PostgresExplainRunner --no-build -- --jobs=1000000 2>&1 | Out-File -Encoding utf8 (Join-Path $PSScriptRoot "postgres-explain/run-console-1M.txt")

$final = Join-Path $PSScriptRoot "execution/final"
Log "=== Execution scenario A 100K x1 postgres/mysql ==="
foreach ($p in @("postgres","mysql")) {
  dotnet run -c Release --project tools/DotnetJobKit.ExecutionPerfRunner --no-build -- --scenario=A --jobs=100000 --iterations=1 --providers=$p --out-dir="$final" 2>&1 | Tee-Object -FilePath (Join-Path $final "scenario-A-$p-console.txt")
}

Log "=== SQLite scenario D + C retry ==="
dotnet run -c Release --project tools/DotnetJobKit.ExecutionPerfRunner --no-build -- --scenario=D --iterations=1 --providers=sqlite --out-dir="$final" 2>&1 | Tee-Object -FilePath (Join-Path $final "scenario-D-retry-console.txt")
dotnet run -c Release --project tools/DotnetJobKit.ExecutionPerfRunner --no-build -- --scenario=C --jobs=300 --max-wait-minutes=90 --iterations=1 --providers=sqlite --out-dir="$final" 2>&1 | Tee-Object -FilePath (Join-Path $final "scenario-C-retry-console.txt")

Log "=== NEW kernel 10K vs baseline pair ==="
dotnet run -c Release --project tools/DotnetJobKit.ExecutionPerfRunner --no-build -- --scenario=A --jobs=10000 --iterations=1 --providers=sqlite --out-dir="$final" 2>&1 | Tee-Object -FilePath (Join-Path $final "scenario-A-10k-new-console.txt")

Log "=== DONE ==="
