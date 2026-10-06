using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.IntegrationTests;

public sealed record ScaleJob(int Sequence);

public sealed class ScaleJobHandler : IJobHandler<ScaleJob>
{
    public static int Completed;

    public ValueTask HandleAsync(ScaleJob job, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Completed);
        return ValueTask.CompletedTask;
    }
}

public class MegaScaleTests
{
    [Fact]
    public async Task Sqlite_large_backlog_claim_stays_bounded()
    {
        var target = StressTestSettings.MegaJobCount;
        var dbPath = Path.Combine(Path.GetTempPath(), $"djk-mega-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={dbPath}";

        await using (var connection = new SqliteConnection(connectionString))
        {
            await connection.OpenAsync();
            await SqliteJobBulkInserter.InsertReadyJobsAsync(
                connection,
                target,
                "default",
                "scale.job",
                1,
                "{}",
                DateTimeOffset.UtcNow,
                maxAttempts: 1,
                batchSize: 20_000,
                progress: null,
                CancellationToken.None);
        }

        using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions
        {
            ConnectionString = connectionString,
            UseBeginImmediate = true,
            Synchronous = 0,
        }));

        var before = GC.GetTotalMemory(forceFullCollection: true);
        var claimed = await store.ClaimAsync(
            ["default"],
            16,
            DateTimeOffset.UtcNow,
            TimeSpan.FromMinutes(1),
            CancellationToken.None);
        var after = GC.GetTotalMemory(forceFullCollection: false);

        Assert.Equal(16, claimed.Count);
        Assert.True(after - before < 50 * 1024 * 1024, "Claim should not allocate large memory proportional to backlog.");

        await using (var connection = new SqliteConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM Jobs WHERE State = 0;";
            var ready = (long)(await cmd.ExecuteScalarAsync() ?? 0L);
            Assert.Equal(target - 16, ready);
        }

        try
        {
            File.Delete(dbPath);
            File.Delete(dbPath + "-wal");
            File.Delete(dbPath + "-shm");
        }
        catch
        {
        }
    }

    [Fact]
    public async Task Runtime_processes_batch_from_large_sqlite_backlog()
    {
        if (!StressTestSettings.RunMegaTenMillion)
            return;

        var target = 10_000_000;
        var processCount = Math.Min(5000, target);
        var dbPath = Path.Combine(Path.GetTempPath(), $"djk-mega-run-{Guid.NewGuid():N}.db");

        await using (var connection = new SqliteConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync();
            await SqliteJobBulkInserter.InsertReadyJobsAsync(
                connection,
                target,
                "default",
                "scale.job",
                1,
                "{}",
                DateTimeOffset.UtcNow,
                maxAttempts: 1,
                batchSize: 50_000,
                cancellationToken: CancellationToken.None);
        }

        ScaleJobHandler.Completed = 0;
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobKit(o =>
        {
            o.MaxConcurrency = 16;
            o.Retention.DeleteOnSuccess = true;
        });
        builder.Services.AddDotnetJobKitSqlite($"Data Source={dbPath}");
        builder.Services.AddDotnetJobHandler<ScaleJob, ScaleJobHandler>("scale.job", "default");

        using var host = builder.Build();
        await host.StartAsync();

        var deadline = DateTime.UtcNow.AddMinutes(30);
        while (ScaleJobHandler.Completed < processCount && DateTime.UtcNow < deadline)
            await Task.Delay(250);

        await host.StopAsync();
        Assert.True(ScaleJobHandler.Completed >= processCount * 0.95,
            $"Expected at least {processCount} completions, got {ScaleJobHandler.Completed}.");
    }
}
