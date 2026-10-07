using System.Diagnostics;
using System.Text.Json;
using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Diagnostics;
using DotnetJobKit.Execution;
using DotnetJobKit.Handlers;
using DotnetJobKit.MySql;
using DotnetJobKit.PostgreSql;
using DotnetJobKit.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var commandLine = Environment.GetCommandLineArgs().Skip(1).ToArray();
var scenario = ParseString(commandLine, "--scenario", "A").ToUpperInvariant();
var jobs = ParseInt(commandLine, "--jobs", DefaultJobsForScenario(scenario));
var providers = ParseProviders(commandLine);
var iterations = ParseInt(commandLine, "--iterations", 3);
var maxConcurrency = ParseInt(commandLine, "--concurrency", 32);
var maxWaitMinutes = ParseInt(commandLine, "--max-wait-minutes", DefaultWaitMinutes(scenario, jobs));
var outDir = ParseString(commandLine, "--out-dir", Path.Combine(FindRepoRoot(), "docs", "benchmarks", "execution", "after"));

Directory.CreateDirectory(outDir);
Console.WriteLine(
    $"ExecutionPerfRunner scenario={scenario} jobs={jobs:N0} providers=[{string.Join(", ", providers)}] iterations={iterations} maxWait={maxWaitMinutes}m out={outDir}");

foreach (var provider in providers)
{
    for (var run = 1; run <= iterations; run++)
    {
        try
        {
            var result = await RunProviderAsync(provider, scenario, jobs, maxConcurrency, maxWaitMinutes, run);
            var fileName = iterations == 1 && scenario == "A"
                ? $"{provider}-run{run}.json"
                : $"{provider}-scenario{scenario}-run{run}.json";
            var path = Path.Combine(outDir, fileName);
            await File.WriteAllTextAsync(path, JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));
            Console.WriteLine(
                $"[{provider} scenario {scenario} run {run}] {result.JobsPerSecond:N0} jobs/sec elapsed={result.ElapsedSeconds:N1}s succeeded={result.JobsSucceeded}/{result.JobsSubmitted} maintJobs={result.MaintenanceJobs}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FAIL] {provider} scenario {scenario} run {run}: {ex.Message}");
            await File.WriteAllTextAsync(
                Path.Combine(outDir, $"{provider}-scenario{scenario}-run{run}-error.txt"),
                ex.ToString());
        }
    }
}

static int DefaultJobsForScenario(string scenario) => scenario switch
{
    "B" => 10_000,
    "C" => 1_000,
    "D" => 5_000,
    _ => 100_000,
};

static int DefaultWaitMinutes(string scenario, int jobs) => scenario switch
{
    "B" => 60,
    "C" => 30,
    "D" => 45,
    _ => jobs >= 50_000 ? 180 : 60,
};

static async Task<ExecutionPerfResult> RunProviderAsync(
    string provider,
    string scenario,
    int jobs,
    int maxConcurrency,
    int maxWaitMinutes,
    int run)
{
    var proc = Process.GetCurrentProcess();
    GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
    var memBefore = proc.WorkingSet64;
    var cpuBefore = proc.TotalProcessorTime;

    ScenarioRuntime.Reset(scenario);
    using var host = BuildHost(provider, scenario, maxConcurrency, out var setup);
    await setup(jobs);
    var expectedTotal = ScenarioRuntime.ExpectedTotal > 0 ? ScenarioRuntime.ExpectedTotal : jobs;
    JobStoreDiagnostics.Reset();
    ExecutionKernelDiagnostics.Reset();

    var sw = Stopwatch.StartNew();
    await host.StartAsync();

    if (scenario == "C")
        _ = ScenarioRuntime.StartCancellationLoop(host, jobs);

    var deadline = maxWaitMinutes <= 0
        ? DateTime.MaxValue
        : DateTime.UtcNow.AddMinutes(maxWaitMinutes);
    while (ScenarioRuntime.Completed < expectedTotal && DateTime.UtcNow < deadline)
        await Task.Delay(100);

    await host.StopAsync();
    sw.Stop();

    var memAfter = proc.WorkingSet64;
    var cpuAfter = proc.TotalProcessorTime;

    return new ExecutionPerfResult(
        provider,
        scenario,
        run,
        jobs,
        maxConcurrency,
        ScenarioRuntime.Completed,
        expectedTotal,
        sw.Elapsed.TotalSeconds,
        ScenarioRuntime.Completed / Math.Max(sw.Elapsed.TotalSeconds, 0.001),
        memBefore,
        memAfter,
        (cpuAfter - cpuBefore).TotalMilliseconds,
        JobStoreDiagnostics.SqlCommandCount,
        ExecutionKernelDiagnostics.MaintenanceBatches,
        ExecutionKernelDiagnostics.MaintenanceJobs,
        ExecutionKernelDiagnostics.DeadlineEventsProcessed,
        ExecutionKernelDiagnostics.StaleDeadlineEventsDiscarded,
        ExecutionKernelDiagnostics.SchedulerWakeups,
        ScenarioRuntime.Cancelled,
        ScenarioRuntime.ContinuationsObserved,
        GC.CollectionCount(0),
        GC.CollectionCount(1),
        GC.CollectionCount(2));
}

static IHost BuildHost(string provider, string scenario, int maxConcurrency, out Func<int, Task> setup)
{
    if (scenario == "D" && provider is not "sqlite")
        throw new InvalidOperationException("Scenario D (continuations) is implemented for sqlite only in ExecutionPerfRunner.");

    var builder = Host.CreateApplicationBuilder();
    builder.Services.AddDotnetJobKit(o =>
    {
        o.MaxConcurrency = maxConcurrency;
        o.Queues = ["default"];
        o.Retention.DeleteOnSuccess = true;
        switch (scenario)
        {
            case "B":
                o.LeaseDuration = TimeSpan.FromSeconds(2);
                o.LeaseRenewInterval = TimeSpan.FromMilliseconds(400);
                o.CancellationPollInterval = TimeSpan.FromSeconds(5);
                break;
            case "C":
                o.LeaseDuration = TimeSpan.FromMinutes(2);
                o.LeaseRenewInterval = TimeSpan.FromSeconds(5);
                o.CancellationPollInterval = TimeSpan.FromMilliseconds(200);
                break;
            default:
                o.LeaseDuration = TimeSpan.FromSeconds(30);
                o.LeaseRenewInterval = TimeSpan.FromSeconds(5);
                o.CancellationPollInterval = TimeSpan.FromSeconds(5);
                break;
        }
    });

    switch (scenario)
    {
        case "A":
            builder.Services.AddDotnetJobHandler<PerfNoOpJob, PerfNoOpHandler>("perf.noop", "default");
            break;
        case "B":
            builder.Services.AddDotnetJobHandler<PerfSlowJob, PerfSlowHandler>("perf.slow", "default");
            break;
        case "C":
            builder.Services.AddDotnetJobHandler<PerfSlowJob, PerfSlowHandler>("perf.slow", "default");
            break;
        case "D":
            builder.Services.AddDotnetJobHandler<PerfNoOpJob, PerfNoOpHandler>("perf.noop", "default");
            builder.Services.AddDotnetJobHandler<PerfChildJob, PerfChildHandler>("perf.child", "default");
            break;
        default:
            throw new InvalidOperationException($"Unknown scenario {scenario}");
    }

    switch (provider)
    {
        case "sqlite":
        {
            var path = Path.Combine(Path.GetTempPath(), $"djk-exec-{Guid.NewGuid():N}.db");
            var cs = $"Data Source={path}";
            builder.Services.AddDotnetJobKitSqlite(cs);
            setup = count => SetupSqliteAsync(cs, scenario, count);
            break;
        }
        case "postgres":
        {
            var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_PG_CONNECTION")
                ?? throw new InvalidOperationException("DOTNETJOBKIT_PG_CONNECTION required");
            builder.Services.AddDotnetJobKitPostgreSql(cs);
            setup = count => SetupPostgresAsync(cs, scenario, count);
            break;
        }
        case "mysql":
        {
            var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_MYSQL_CONNECTION")
                ?? throw new InvalidOperationException("DOTNETJOBKIT_MYSQL_CONNECTION required");
            builder.Services.AddDotnetJobKitMySql(cs);
            setup = count => SetupMySqlAsync(cs, scenario, count);
            break;
        }
        default:
            throw new InvalidOperationException($"Unknown provider {provider}");
    }

    return builder.Build();
}

static async Task SetupSqliteAsync(string cs, string scenario, int count)
{
    await using var connection = new Microsoft.Data.Sqlite.SqliteConnection(cs);
    await connection.OpenAsync();
    switch (scenario)
    {
        case "D":
            await SqliteJobBulkInserter.InsertReadyJobsAsync(
                connection, count, "default", "perf.noop", 1, "{}", DateTimeOffset.UtcNow, 3, batchSize: 5_000,
                continuationContractName: "perf.child", continuationPayload: "{}");
            ScenarioRuntime.ExpectedTotal = count * 2;
            break;
        case "B":
            await SqliteJobBulkInserter.InsertReadyJobsAsync(
                connection, count, "default", "perf.slow", 1, "{}", DateTimeOffset.UtcNow, 3, batchSize: 10_000);
            ScenarioRuntime.ExpectedTotal = count;
            break;
        case "C":
            ScenarioRuntime.ExpectedTotal = count;
            ScenarioRuntime.PendingCancelIds = new List<Guid>(count);
            break;
        default:
            await SqliteJobBulkInserter.InsertReadyJobsAsync(
                connection, count, "default", "perf.noop", 1, "{}", DateTimeOffset.UtcNow, 3, batchSize: 20_000);
            ScenarioRuntime.ExpectedTotal = count;
            break;
    }

    if (scenario == "C")
        await ScenarioRuntime.EnqueueSlowJobsAsync(cs, count, isSqlite: true);
}

static async Task SetupPostgresAsync(string cs, string scenario, int count)
{
    await using var connection = new Npgsql.NpgsqlConnection(cs);
    await connection.OpenAsync();
    PostgreSqlJobStore.EnsureSchema(connection);
    var contract = scenario is "B" or "C" ? "perf.slow" : "perf.noop";
    await PostgreSqlJobBulkInserter.InsertReadyJobsAsync(
        connection, count, "default", contract, 1, "{}", DateTimeOffset.UtcNow, 3, batchSize: 5_000);
    ScenarioRuntime.ExpectedTotal = count;
    if (scenario == "C")
        await ScenarioRuntime.EnqueueSlowJobsPostgresAsync(cs, count);
}

static async Task SetupMySqlAsync(string cs, string scenario, int count)
{
    await using var connection = new MySqlConnector.MySqlConnection(cs);
    await connection.OpenAsync();
    MySqlJobStore.EnsureSchema(connection);
    var contract = scenario is "B" or "C" ? "perf.slow" : "perf.noop";
    await MySqlJobBulkInserter.InsertReadyJobsAsync(
        connection, count, "default", contract, 1, "{}", DateTimeOffset.UtcNow, 3, batchSize: 5_000);
    ScenarioRuntime.ExpectedTotal = count;
    if (scenario == "C")
        await ScenarioRuntime.EnqueueSlowJobsMySqlAsync(cs, count);
}

static int ParseInt(string[] commandLine, string flag, int defaultValue)
{
    var arg = commandLine.FirstOrDefault(a => a.StartsWith(flag + "=", StringComparison.OrdinalIgnoreCase));
    return arg is null ? defaultValue : int.Parse(arg[(flag.Length + 1)..], System.Globalization.CultureInfo.InvariantCulture);
}

static string ParseString(string[] commandLine, string flag, string defaultValue)
{
    var arg = commandLine.FirstOrDefault(a => a.StartsWith(flag + "=", StringComparison.OrdinalIgnoreCase));
    return arg is null ? defaultValue : arg[(flag.Length + 1)..];
}

static string[] ParseProviders(string[] a)
{
    var arg = a.FirstOrDefault(x => x.StartsWith("--providers=", StringComparison.OrdinalIgnoreCase));
    return arg is null ? ["sqlite"] : arg["--providers=".Length..].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}

static string FindRepoRoot()
{
    var dir = AppContext.BaseDirectory;
    while (!string.IsNullOrEmpty(dir))
    {
        if (Directory.Exists(Path.Combine(dir, "docs")) && Directory.Exists(Path.Combine(dir, "src")))
            return dir;
        dir = Directory.GetParent(dir)?.FullName ?? string.Empty;
    }

    return Directory.GetCurrentDirectory();
}

public sealed record PerfNoOpJob;

public sealed class PerfNoOpHandler : IJobHandler<PerfNoOpJob>
{
    public Task HandleAsync(PerfNoOpJob job, JobContext context, CancellationToken cancellationToken)
    {
        ScenarioRuntime.RecordSuccess();
        return Task.CompletedTask;
    }
}

public sealed record PerfChildJob;

public sealed class PerfChildHandler : IJobHandler<PerfChildJob>
{
    public Task HandleAsync(PerfChildJob job, JobContext context, CancellationToken cancellationToken)
    {
        ScenarioRuntime.RecordSuccess(isContinuation: true);
        return Task.CompletedTask;
    }
}

public sealed record PerfSlowJob(int Id);

public sealed class PerfSlowHandler : IJobHandler<PerfSlowJob>
{
    public async Task HandleAsync(PerfSlowJob job, JobContext context, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(ScenarioRuntime.SlowHandlerDelayMs, cancellationToken).ConfigureAwait(false);
            ScenarioRuntime.RecordSuccess();
        }
        catch (OperationCanceledException)
        {
            ScenarioRuntime.RecordCancelled();
            throw;
        }
    }
}

public static class ScenarioRuntime
{
    public static int ExpectedTotal;
    public static int Completed;
    public static int Cancelled;
    public static int ContinuationsObserved;
    public static List<Guid>? PendingCancelIds;
    public static int SlowHandlerDelayMs = 80;

    public static void Reset(string scenario)
    {
        ExpectedTotal = 0;
        Completed = 0;
        Cancelled = 0;
        ContinuationsObserved = 0;
        PendingCancelIds = null;
        SlowHandlerDelayMs = scenario switch
        {
            "B" => 80,
            "C" => 400,
            _ => 0,
        };
    }

    public static void RecordSuccess(bool isContinuation = false)
    {
        Interlocked.Increment(ref Completed);
        if (isContinuation)
            Interlocked.Increment(ref ContinuationsObserved);
    }

    public static void RecordCancelled() => Interlocked.Increment(ref Cancelled);

    public static Task StartCancellationLoop(IHost host, int jobs)
    {
        return Task.Run(async () =>
        {
            await Task.Delay(500).ConfigureAwait(false);
            var submitter = host.Services.GetRequiredService<IJobSubmitter>();
            var ids = PendingCancelIds;
            if (ids is null || ids.Count == 0)
                return;
            for (var i = 0; i < ids.Count; i += 2)
                await submitter.CancelAsync(ids[i]).ConfigureAwait(false);
        });
    }

    public static async Task EnqueueSlowJobsAsync(string cs, int count, bool isSqlite)
    {
        using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        PendingCancelIds = new List<Guid>(count);
        for (var i = 0; i < count; i++)
        {
            var id = await store.SubmitAsync(new JobSubmitRequest
            {
                Queue = "default",
                ContractName = "perf.slow",
                ContractVersion = 1,
                Payload = JsonSerializer.Serialize(new PerfSlowJob(i)),
            }, now, CancellationToken.None);
            PendingCancelIds.Add(id);
        }
    }

    public static async Task EnqueueSlowJobsPostgresAsync(string cs, int count)
    {
        using var store = new PostgreSqlJobStore(Options.Create(new PostgreSqlJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        PendingCancelIds = new List<Guid>(count);
        for (var i = 0; i < count; i++)
        {
            var id = await store.SubmitAsync(new JobSubmitRequest
            {
                Queue = "default",
                ContractName = "perf.slow",
                ContractVersion = 1,
                Payload = JsonSerializer.Serialize(new PerfSlowJob(i)),
            }, now, CancellationToken.None);
            PendingCancelIds.Add(id);
        }
    }

    public static async Task EnqueueSlowJobsMySqlAsync(string cs, int count)
    {
        using var store = new MySqlJobStore(Options.Create(new MySqlJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        PendingCancelIds = new List<Guid>(count);
        for (var i = 0; i < count; i++)
        {
            var id = await store.SubmitAsync(new JobSubmitRequest
            {
                Queue = "default",
                ContractName = "perf.slow",
                ContractVersion = 1,
                Payload = JsonSerializer.Serialize(new PerfSlowJob(i)),
            }, now, CancellationToken.None);
            PendingCancelIds.Add(id);
        }
    }
}

public sealed record ExecutionPerfResult(
    string Provider,
    string Scenario,
    int Run,
    int JobsSubmitted,
    int MaxConcurrency,
    int JobsSucceeded,
    int JobsExpected,
    double ElapsedSeconds,
    double JobsPerSecond,
    long RssBeforeBytes,
    long RssAfterBytes,
    double CpuMs,
    int SqlCommandCount,
    long MaintenanceBatches,
    long MaintenanceJobs,
    long DeadlineEventsProcessed,
    long StaleDeadlineEventsDiscarded,
    long SchedulerWakeups,
    int JobsCancelled,
    int ContinuationsCompleted,
    int Gen0Collections,
    int Gen1Collections,
    int Gen2Collections);
