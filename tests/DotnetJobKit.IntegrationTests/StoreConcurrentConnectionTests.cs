using DotnetJobKit.Configuration;
using DotnetJobKit.MySql;
using DotnetJobKit.PostgreSql;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.IntegrationTests;

public class StoreConcurrentConnectionTests
{
    [Fact]
    public async Task PostgreSql_single_store_supports_32_concurrent_gets()
    {
        var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_PG_CONNECTION");
        if (string.IsNullOrWhiteSpace(cs))
            return;

        using var store = new PostgreSqlJobStore(Options.Create(new PostgreSqlJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        var id = await store.SubmitAsync(new Abstractions.JobSubmitRequest
        {
            Queue = "default",
            ContractName = "c",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        var tasks = Enumerable.Range(0, 32).Select(_ => store.GetAsync(id, CancellationToken.None));
        var results = await Task.WhenAll(tasks);
        Assert.All(results, r => Assert.NotNull(r));
    }

    [Fact]
    public async Task MySql_single_store_supports_32_concurrent_gets()
    {
        var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_MYSQL_CONNECTION");
        if (string.IsNullOrWhiteSpace(cs))
            return;

        using var store = new MySqlJobStore(Options.Create(new MySqlJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        var id = await store.SubmitAsync(new Abstractions.JobSubmitRequest
        {
            Queue = "default",
            ContractName = "c",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        var tasks = Enumerable.Range(0, 32).Select(_ => store.GetAsync(id, CancellationToken.None));
        var results = await Task.WhenAll(tasks);
        Assert.All(results, r => Assert.NotNull(r));
    }
}
