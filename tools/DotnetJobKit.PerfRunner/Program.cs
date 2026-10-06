using System.Diagnostics;
using System.Text;
using System.Text.Json;
using DotnetJobKit.Abstractions;
using DotnetJobKit.MySql;
using DotnetJobKit.PostgreSql;
using DotnetJobKit.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Npgsql;

var jobs = ParseIntArg("--jobs", Environment.GetEnvironmentVariable("DOTNETJOBKIT_MEGA_JOBS"), 1_000_000);
var providers = ParseProviders(args);
var claimOnly = args.Any(a => a.Equals("--claim-only", StringComparison.OrdinalIgnoreCase));
var reportPath = Path.Combine(FindSolutionRoot(), "docs", "PERFORMANCE-REPORT.md");
var results = new List<PerfResult>();

Console.WriteLine($"DotnetJobKit PerfRunner — jobs={jobs:N0}, providers=[{string.Join(", ", providers)}]{(claimOnly ? ", claim-only" : "")}");

foreach (var provider in providers)
{
    try
    {
        var result = provider switch
        {
            "sqlite" => claimOnly ? throw new NotSupportedException("claim-only supports postgres only") : await RunSqliteAsync(jobs),
            "postgres" => claimOnly ? await RunPostgresClaimOnlyAsync(jobs) : await RunPostgresAsync(jobs),
            "mysql" => claimOnly ? throw new NotSupportedException("claim-only supports postgres only") : await RunMySqlAsync(jobs),
            _ => throw new InvalidOperationException($"Unknown provider {provider}"),
        };
        results.Add(result);
        PrintResult(result);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[FAIL] {provider}: {ex.GetType().Name}: {ex.Message}");
        results.Add(new PerfResult(provider, jobs, false, $"{ex.GetType().Name}: {ex.Message}", TimeSpan.Zero, 0, TimeSpan.Zero, 0, 0, 0, 0));
    }
}

var reportSuffix = jobs >= 10_000_000 ? "-10M" : "";
WriteReport(reportPath.Replace(".md", $"{reportSuffix}.md"), results, jobs);
Console.WriteLine($"Report written (jobs={jobs:N0})");

static async Task<PerfResult> RunSqliteAsync(int jobs)
{
    var path = Path.Combine(Path.GetTempPath(), $"djk-perf-{Guid.NewGuid():N}.db");
    var cs = $"Data Source={path}";
    await using var connection = new SqliteConnection(cs);
    await connection.OpenAsync();

    var proc = Process.GetCurrentProcess();
    GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
    var memBefore = proc.WorkingSet64;
    var cpuBefore = proc.TotalProcessorTime;

    var sw = Stopwatch.StartNew();
    await SqliteJobBulkInserter.InsertReadyJobsAsync(
        connection, jobs, "default", "perf.job", 1, "{}", DateTimeOffset.UtcNow, 1, batchSize: 20_000);
    sw.Stop();
    var insertRate = jobs / sw.Elapsed.TotalSeconds;

    using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions { ConnectionString = cs }));
    var claimSw = Stopwatch.StartNew();
    var claimed = await store.ClaimAsync(["default"], 16, DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1), CancellationToken.None);
    claimSw.Stop();

    var memAfter = proc.WorkingSet64;
    var cpuAfter = proc.TotalProcessorTime;
    var cpuMs = (cpuAfter - cpuBefore).TotalMilliseconds;

    try { File.Delete(path); } catch { }

    return new PerfResult(
        "sqlite", jobs, true, null,
        sw.Elapsed, insertRate,
        claimSw.Elapsed, claimed.Count,
        memBefore, memAfter, cpuMs);
}

static async Task<PerfResult> RunPostgresClaimOnlyAsync(int jobs)
{
    var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_PG_CONNECTION")
        ?? "Host=localhost;Port=5432;Username=postgres;Password=admin;Database=postgres";
    var dbCs = ReplaceDatabase(cs, "dotnetjobkit_perf");

    var proc = Process.GetCurrentProcess();
    GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
    var memBefore = proc.WorkingSet64;
    var cpuBefore = proc.TotalProcessorTime;

    using var store = new PostgreSqlJobStore(Options.Create(new PostgreSqlJobStoreOptions { ConnectionString = dbCs }));
    var claimSw = Stopwatch.StartNew();
    var claimed = await store.ClaimAsync(["default"], 16, DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1), CancellationToken.None);
    claimSw.Stop();

    var memAfter = proc.WorkingSet64;
    var cpuMs = (proc.TotalProcessorTime - cpuBefore).TotalMilliseconds;

    return new PerfResult(
        "postgres", jobs, true, null,
        TimeSpan.Zero, 0,
        claimSw.Elapsed, claimed.Count,
        memBefore, memAfter, cpuMs);
}

static async Task<PerfResult> RunPostgresAsync(int jobs)
{
    var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_PG_CONNECTION")
        ?? "Host=localhost;Port=5432;Username=postgres;Password=admin;Database=postgres";

    await using var admin = new NpgsqlConnection(cs);
    await admin.OpenAsync();
    await EnsurePostgresDatabaseAsync(admin);

    var dbCs = ReplaceDatabase(cs, "dotnetjobkit_perf");
    var dbCsb = new NpgsqlConnectionStringBuilder(dbCs) { CommandTimeout = 0 };
    await using var connection = new NpgsqlConnection(dbCsb.ConnectionString);
    await connection.OpenAsync();
    await PostgreSqlJobBulkInserter.TruncateAsync(connection);

    return await RunProviderCore("postgres", jobs, async () =>
    {
        await PostgreSqlJobBulkInserter.InsertReadyJobsAsync(
            connection, jobs, "default", "perf.job", 1, "{}", DateTimeOffset.UtcNow, 1, batchSize: 10_000);
        await using var analyze = new NpgsqlCommand("ANALYZE djk_jobs;", connection);
        await analyze.ExecuteNonQueryAsync();
    }, async () =>
    {
        using var store = new PostgreSqlJobStore(Options.Create(new PostgreSqlJobStoreOptions { ConnectionString = dbCs }));
        return await store.ClaimAsync(["default"], 16, DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1), CancellationToken.None);
    });
}

static async Task<PerfResult> RunMySqlAsync(int jobs)
{
    var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_MYSQL_CONNECTION")
        ?? "Server=localhost;Port=3306;Database=Adastra_QA;User=admin;Password=admin";

    await using var connection = new MySqlConnection(cs);
    await connection.OpenAsync();
    await MySqlJobBulkInserter.TruncateAsync(connection);

    return await RunProviderCore("mysql", jobs, async () =>
    {
        await MySqlJobBulkInserter.InsertReadyJobsAsync(
            connection, jobs, "default", "perf.job", 1, "{}", DateTimeOffset.UtcNow, 1, batchSize: 2_000);
    }, async () =>
    {
        using var store = new MySqlJobStore(Options.Create(new MySqlJobStoreOptions { ConnectionString = cs }));
        return await store.ClaimAsync(["default"], 16, DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1), CancellationToken.None);
    });
}

static async Task<PerfResult> RunProviderCore(
    string name,
    int jobs,
    Func<Task> insert,
    Func<Task<IReadOnlyList<ClaimedJob>>> claim)
{
    var proc = Process.GetCurrentProcess();
    GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
    var memBefore = proc.WorkingSet64;
    var cpuBefore = proc.TotalProcessorTime;

    var sw = Stopwatch.StartNew();
    await insert();
    sw.Stop();
    var insertRate = jobs / sw.Elapsed.TotalSeconds;

    var claimSw = Stopwatch.StartNew();
    var claimed = await claim();
    claimSw.Stop();

    var memAfter = proc.WorkingSet64;
    var cpuMs = (proc.TotalProcessorTime - cpuBefore).TotalMilliseconds;

    return new PerfResult(name, jobs, true, null, sw.Elapsed, insertRate, claimSw.Elapsed, claimed.Count, memBefore, memAfter, cpuMs);
}

static async Task EnsurePostgresDatabaseAsync(NpgsqlConnection admin)
{
    await using var cmd = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname = 'dotnetjobkit_perf';", admin);
    var exists = await cmd.ExecuteScalarAsync();
    if (exists is null)
    {
        await using var create = new NpgsqlCommand("CREATE DATABASE dotnetjobkit_perf;", admin);
        await create.ExecuteNonQueryAsync();
    }
}

static string ReplaceDatabase(string connectionString, string database)
{
    var builder = new NpgsqlConnectionStringBuilder(connectionString) { Database = database };
    return builder.ConnectionString;
}

static void PrintResult(PerfResult r)
{
    if (!r.Success)
        return;

    Console.WriteLine(
        $"[{r.Provider}] insert {r.InsertDuration.TotalSeconds:F1}s ({r.InsertJobsPerSecond:N0} jobs/s) | " +
        $"claim16 {r.ClaimDuration.TotalMilliseconds:F1}ms | WS {r.MemoryAfterBytes / (1024 * 1024):F0} MB");
}

static void WriteReport(string path, List<PerfResult> results, int jobs)
{
    var sb = new StringBuilder();
    sb.AppendLine("# DotnetJobKit performance report");
    sb.AppendLine();
    sb.AppendLine($"Generated (UTC): `{DateTime.UtcNow:O}`");
    sb.AppendLine($"Job count per provider test: **{jobs:N0}**");
    sb.AppendLine();
    sb.AppendLine("## Summary");
    sb.AppendLine();
    sb.AppendLine("| Provider | OK | Insert time | Insert throughput | Claim 16 jobs | RAM delta (WS) | CPU (process ms) |");
    sb.AppendLine("|---|---:|---:|---:|---:|---:|---:|");
    foreach (var r in results)
    {
        if (!r.Success)
        {
            sb.AppendLine($"| {r.Provider} | no | — | — | — | — | `{r.Error}` |");
            continue;
        }

        var insertTime = r.InsertDuration <= TimeSpan.Zero ? "—" : $"{r.InsertDuration.TotalSeconds:F1}s";
        var insertRate = r.InsertJobsPerSecond <= 0 ? "—" : $"{r.InsertJobsPerSecond:N0}/s";
        sb.AppendLine(string.Join(" | ",
        [
            r.Provider,
            "yes",
            insertTime,
            insertRate,
            $"{r.ClaimDuration.TotalMilliseconds:F1}ms",
            $"{(r.MemoryAfterBytes - r.MemoryBeforeBytes) / (1024.0 * 1024.0):F1} MB",
            $"{r.CpuMilliseconds:F0}ms",
        ]));
    }

    sb.AppendLine();
    sb.AppendLine("## How to reproduce");
    sb.AppendLine();
    sb.AppendLine("```powershell");
    sb.AppendLine("cd \"github projects/DotnetJobKit\"");
    sb.AppendLine("$env:DOTNETJOBKIT_PG_CONNECTION=\"Host=localhost;Port=5432;Username=postgres;Password=<secret>;Database=postgres\"");
    sb.AppendLine("$env:DOTNETJOBKIT_MYSQL_CONNECTION=\"Server=localhost;Port=3306;Database=Adastra_QA;User=admin;Password=<secret>\"");
    sb.AppendLine("dotnet run -c Release --project tools/DotnetJobKit.PerfRunner -- --jobs 1000000 --providers sqlite,postgres,mysql");
    sb.AppendLine("# 10 million:");
    sb.AppendLine("dotnet run -c Release --project tools/DotnetJobKit.PerfRunner -- --jobs 10000000 --providers postgres");
    sb.AppendLine("```");
    sb.AppendLine();
    var jsonName = Path.GetFileName(Path.ChangeExtension(path, ".json"));
    sb.AppendLine($"Raw JSON snapshot is written beside this file as `{jsonName}`.");
    Directory.CreateDirectory(Path.GetDirectoryName(path)!);
    File.WriteAllText(path, sb.ToString());
    File.WriteAllText(Path.ChangeExtension(path, ".json"), JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));
}

static int ParseIntArg(string name, string? envFallback, int defaultValue)
{
    foreach (var arg in Environment.GetCommandLineArgs())
    {
        if (arg.StartsWith(name + "=", StringComparison.OrdinalIgnoreCase))
            return int.Parse(arg[(name.Length + 1)..]);
    }

    if (int.TryParse(envFallback, out var env))
        return env;

    return defaultValue;
}

static string[] ParseProviders(string[] args)
{
    var raw = args.FirstOrDefault(a => a.StartsWith("--providers=", StringComparison.OrdinalIgnoreCase))
        ?? "--providers=sqlite,postgres,mysql";
    return raw.Split('=')[1].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}

static string FindSolutionRoot()
{
    var dir = AppContext.BaseDirectory;
    while (!string.IsNullOrEmpty(dir))
    {
        if (File.Exists(Path.Combine(dir, "DotnetJobKit.sln")))
            return dir;
        dir = Directory.GetParent(dir)?.FullName ?? string.Empty;
    }

    return Directory.GetCurrentDirectory();
}

internal sealed record PerfResult(
    string Provider,
    int JobCount,
    bool Success,
    string? Error,
    TimeSpan InsertDuration,
    double InsertJobsPerSecond,
    TimeSpan ClaimDuration,
    int ClaimedCount,
    long MemoryBeforeBytes,
    long MemoryAfterBytes,
    double CpuMilliseconds);
