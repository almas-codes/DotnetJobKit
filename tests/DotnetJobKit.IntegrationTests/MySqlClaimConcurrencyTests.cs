using DotnetJobKit.Configuration;
using DotnetJobKit.MySql;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.IntegrationTests;

public class MySqlClaimConcurrencyTests
{
    [Fact]
    public async Task Parallel_claims_do_not_deadlock()
    {
        var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_MYSQL_CONNECTION");
        if (string.IsNullOrWhiteSpace(cs))
            return;

        await using var setup = new MySqlConnector.MySqlConnection(cs);
        await setup.OpenAsync();
        MySqlJobStore.EnsureSchema(setup);
        await MySqlJobBulkInserter.TruncateAsync(setup);
        await MySqlJobBulkInserter.InsertReadyJobsAsync(
            setup, 200, "default", "perf.job", 1, "{}", DateTimeOffset.UtcNow, 1, batchSize: 50);

        var tasks = Enumerable.Range(0, 16).Select(async _ =>
        {
            using var store = new MySqlJobStore(Options.Create(new MySqlJobStoreOptions { ConnectionString = cs }));
            var total = 0;
            for (var i = 0; i < 20; i++)
            {
                var batch = await store.ClaimAsync(["default"], 1, DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1), CancellationToken.None);
                total += batch.Count;
                if (batch.Count == 0)
                    break;
            }

            return total;
        });

        var results = await Task.WhenAll(tasks);
        Assert.InRange(results.Sum(), 100, 200);
    }
}
