using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net10_0, warmupCount: 1, iterationCount: 3)]
public class SqliteClaimBenchmarks
{
    private string _dbPath = null!;
    private SqliteJobStore _attemptStore = null!;
    private SqliteJobStore _tokenStore = null!;

    [Params(1000, 10000)]
    public int QueueDepth { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"djk-bench-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={_dbPath}";

        using (var connection = new SqliteConnection(connectionString))
        {
            connection.Open();
            using var wipe = connection.CreateCommand();
            wipe.CommandText = "DROP TABLE IF EXISTS Jobs;";
            wipe.ExecuteNonQuery();
        }

        SeedDatabase(connectionString, QueueDepth);

        _attemptStore = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions
        {
            ConnectionString = connectionString,
            OwnershipFencing = OwnershipFencingMode.AttemptCount,
        }));

        _tokenStore = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions
        {
            ConnectionString = connectionString,
            OwnershipFencing = OwnershipFencingMode.LeaseToken,
        }));
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _attemptStore.Dispose();
        _tokenStore.Dispose();
        try
        {
            File.Delete(_dbPath);
        }
        catch
        {
        }
    }

    [IterationSetup]
    public void IterationSetup() => SeedDatabase($"Data Source={_dbPath}", QueueDepth);

    [Benchmark(Baseline = true)]
    public async Task Claim_AttemptCount()
    {
        var now = DateTimeOffset.UtcNow;
        _ = await _attemptStore.ClaimAsync(["default"], 16, now, TimeSpan.FromMinutes(1), CancellationToken.None)
            .ConfigureAwait(false);
    }

    [Benchmark]
    public async Task Claim_LeaseToken()
    {
        var now = DateTimeOffset.UtcNow;
        _ = await _tokenStore.ClaimAsync(["default"], 16, now, TimeSpan.FromMinutes(1), CancellationToken.None)
            .ConfigureAwait(false);
    }

    private static void SeedDatabase(string connectionString, int count)
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        SqliteJobBulkInserter.InsertReadyJobsAsync(
            connection,
            count,
            "default",
            "bench.job",
            1,
            "{}",
            DateTimeOffset.UtcNow,
            maxAttempts: 3,
            batchSize: 5000).GetAwaiter().GetResult();
    }
}
