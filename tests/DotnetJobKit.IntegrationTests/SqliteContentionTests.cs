using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.IntegrationTests;

public class SqliteContentionTests
{
    [Fact]
    public async Task Multiple_connections_claim_distinct_jobs()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"djk-contention-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={dbPath}";

        await using (var connection = new SqliteConnection(connectionString))
        {
            await connection.OpenAsync();
            await SqliteJobBulkInserter.InsertReadyJobsAsync(
                connection,
                200,
                "default",
                "t",
                1,
                "{}",
                DateTimeOffset.UtcNow,
                maxAttempts: 3,
                cancellationToken: CancellationToken.None);
        }

        var workers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions
            {
                ConnectionString = connectionString,
                UseBeginImmediate = true,
            }));

            var claimedIds = new List<Guid>();
            for (var i = 0; i < 50; i++)
            {
                var batch = await store.ClaimAsync(
                    ["default"],
                    4,
                    DateTimeOffset.UtcNow,
                    TimeSpan.FromMinutes(1),
                    CancellationToken.None);
                if (batch.Count == 0)
                    break;

                claimedIds.AddRange(batch.Select(x => x.JobId));
            }

            return claimedIds;
        })).ToArray();

        var results = await Task.WhenAll(workers);
        var all = results.SelectMany(x => x).ToList();
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.True(all.Count >= 150);

        try
        {
            File.Delete(dbPath);
        }
        catch
        {
        }
    }
}
