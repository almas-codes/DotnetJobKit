# Copy to environment.local.ps1 (gitignored) and dot-source before benchmarks:
#   . .\docs\benchmarks\environment.local.ps1

$env:DOTNETJOBKIT_PG_CONNECTION = "Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=postgres"
$env:DOTNETJOBKIT_MYSQL_CONNECTION = "Server=localhost;Port=3306;Database=Adastra_QA;User=admin;Password=YOUR_PASSWORD"
