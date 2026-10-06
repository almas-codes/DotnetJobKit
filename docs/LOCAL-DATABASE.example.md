# Local database settings (example)

Do not commit real passwords. Set environment variables before running perf tests or integration tests against your machine:

```powershell
$env:DOTNETJOBKIT_PG_CONNECTION = "Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=postgres"
$env:DOTNETJOBKIT_MYSQL_CONNECTION = "Server=localhost;Port=3306;Database=Adastra_QA;User=admin;Password=YOUR_PASSWORD"
```

PostgreSQL creates database `dotnetjobkit_perf` automatically. MySQL uses table `djk_jobs` inside your configured database.
