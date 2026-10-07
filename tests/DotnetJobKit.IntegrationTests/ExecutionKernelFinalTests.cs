using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Client;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.Diagnostics;
using DotnetJobKit.PostgreSql;
using DotnetJobKit.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.IntegrationTests;

public class ExecutionKernelFinalTests
{
    [Fact]
    public async Task Sqlite_retention_purge_does_not_remove_active_leased_jobs()
    {
        var path = Path.Combine(Path.GetTempPath(), $"djk-ret-{Guid.NewGuid():N}.db");
        var cs = $"Data Source={path}";
        using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        var leasedId = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "c",
            ContractVersion = 1,
            Payload = "{}",
            EligibleAt = now,
        }, now, CancellationToken.None);
        var readyId = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "c",
            ContractVersion = 1,
            Payload = "{}",
            EligibleAt = now.AddMinutes(10),
        }, now, CancellationToken.None);
        var leased = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Equal(leasedId, leased[0].JobId);

        var succeededId = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "c",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);
        var claimed = (await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None))[0];
        await store.SettleAsync(new SettleRequest
        {
            JobId = claimed.JobId,
            AttemptCount = claimed.AttemptCount,
            LeaseToken = claimed.LeaseToken,
            Outcome = SettleOutcome.Succeeded,
            Now = now,
        }, CancellationToken.None);

        var deleted = await store.DeleteTerminalBatchAsync(
            now.AddMinutes(1),
            new JobRetentionPurge
            {
                SucceededRetention = TimeSpan.Zero,
                FailedRetention = TimeSpan.FromDays(1),
                CancelledRetention = TimeSpan.FromDays(1),
            },
            batchSize: 100,
            CancellationToken.None);

        Assert.True(deleted >= 1);
        Assert.NotNull(await store.GetAsync(readyId, CancellationToken.None));
        Assert.NotNull(await store.GetAsync(leasedId, CancellationToken.None));
    }

    [Fact]
    public async Task Sqlite_mixed_concurrent_get_and_claim_on_shared_store()
    {
        var path = Path.Combine(Path.GetTempPath(), $"djk-mix-{Guid.NewGuid():N}.db");
        var cs = $"Data Source={path}";
        using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        var ids = new List<Guid>();
        for (var i = 0; i < 32; i++)
        {
            ids.Add(await store.SubmitAsync(new JobSubmitRequest
            {
                Queue = "default",
                ContractName = "c",
                ContractVersion = 1,
                Payload = "{}",
            }, now, CancellationToken.None));
        }

        var getTasks = ids.Select(id => store.GetAsync(id, CancellationToken.None));
        var claimTasks = Enumerable.Range(0, 8).Select(_ =>
            store.ClaimAsync(["default"], 4, now, TimeSpan.FromMinutes(1), CancellationToken.None));
        await Task.WhenAll(getTasks);
        var claimResults = await Task.WhenAll(claimTasks);
        Assert.True(claimResults.Sum(r => r.Count) >= 8);
    }

    [Fact]
    public async Task PostgreSql_maintain_32_leases_uses_bounded_commands()
    {
        var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_PG_CONNECTION");
        if (string.IsNullOrWhiteSpace(cs))
            return;

        using var store = new PostgreSqlJobStore(Options.Create(new PostgreSqlJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        var requests = new List<LeaseMaintenanceRequest>();
        for (var i = 0; i < 32; i++)
        {
            _ = await store.SubmitAsync(new JobSubmitRequest
            {
                Queue = "default",
                ContractName = "c",
                ContractVersion = 1,
                Payload = "{}",
            }, now, CancellationToken.None);
            var claimed = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None);
            var job = claimed[0];
            requests.Add(new LeaseMaintenanceRequest(job.JobId, job.AttemptCount, job.LeaseToken, now.AddMinutes(2)));
        }

        JobStoreDiagnostics.Reset();
        var results = await store.MaintainLeasesAsync(requests, CancellationToken.None);
        Assert.Equal(32, results.Count);
        Assert.All(results, r => Assert.True(r.StillOwner));
        Assert.InRange(JobStoreDiagnostics.SqlCommandCount, 1, 8);
    }

}
