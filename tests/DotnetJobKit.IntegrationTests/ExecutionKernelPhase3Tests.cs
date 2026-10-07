using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Diagnostics;
using DotnetJobKit.MySql;
using DotnetJobKit.PostgreSql;
using DotnetJobKit.Sqlite;
using DotnetJobKit.Storage;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.IntegrationTests;

public class ExecutionKernelPhase3Tests
{
    [Fact]
    public async Task Sqlite_maintain_32_leases_uses_bounded_commands()
    {
        var path = Path.Combine(Path.GetTempPath(), $"djk-maint-{Guid.NewGuid():N}.db");
        var cs = $"Data Source={path}";
        using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions { ConnectionString = cs }));
        var now = DateTimeOffset.UtcNow;
        var requests = new List<LeaseMaintenanceRequest>();
        for (var i = 0; i < 32; i++)
        {
            var id = await store.SubmitAsync(new JobSubmitRequest
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

    [Fact]
    public async Task InMemory_stale_worker_cannot_mutate_after_reclaim()
    {
        var store = new InMemoryJobStore();
        var now = DateTimeOffset.UtcNow;
        var jobId = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "c",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        var a = (await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None))[0];
        _ = await store.ClaimAsync(["default"], 1, now.AddMinutes(2), TimeSpan.FromMinutes(1), CancellationToken.None);

        Assert.False(await store.RenewAsync(jobId, a.AttemptCount, a.LeaseToken, now.AddMinutes(5), CancellationToken.None));
        Assert.False(await store.SettleAsync(new SettleRequest
        {
            JobId = jobId,
            AttemptCount = a.AttemptCount,
            LeaseToken = a.LeaseToken,
            Outcome = SettleOutcome.Succeeded,
            Now = now,
        }, CancellationToken.None));

        var staleCommit = new CommitOutcomeRequest(
            jobId,
            a.AttemptCount,
            a.LeaseToken,
            new ExecutionOutcome(ExecutionOutcomeKind.Succeeded, Continuation: new ContinuationSpec("default", "child", 1, "{}")),
            3,
            new RetryPolicyOptions());
        Assert.Equal(CommitOutcomeStatus.StaleOwner, (await store.CommitOutcomeAsync(staleCommit, now, CancellationToken.None)).Status);
    }

    [Fact]
    public async Task InMemory_duplicate_commit_after_crash_is_idempotent()
    {
        var store = new InMemoryJobStore();
        var now = DateTimeOffset.UtcNow;
        var jobId = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "c",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);
        var claimed = (await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None))[0];
        var commit = new CommitOutcomeRequest(
            claimed.JobId,
            claimed.AttemptCount,
            claimed.LeaseToken,
            new ExecutionOutcome(ExecutionOutcomeKind.Succeeded, Continuation: new ContinuationSpec("default", "child", 1, "{}")),
            claimed.MaxAttempts,
            new RetryPolicyOptions());

        var first = await store.CommitOutcomeAsync(commit, now, CancellationToken.None);
        var replay = await store.CommitOutcomeAsync(commit, now, CancellationToken.None);
        Assert.Equal(CommitOutcomeStatus.Committed, first.Status);
        Assert.Equal(CommitOutcomeStatus.AlreadyCommitted, replay.Status);
        Assert.Equal(first.SuccessorJobId, replay.SuccessorJobId);

        var counts = await store.GetCountsByStateAsync(CancellationToken.None);
        Assert.Equal(1, counts.GetValueOrDefault(JobState.Succeeded));
        Assert.Equal(1, counts.GetValueOrDefault(JobState.Ready));
    }

    [Fact]
    public async Task Sqlite_fifty_way_idempotency_creates_one_row()
    {
        var path = Path.Combine(Path.GetTempPath(), $"djk-idem-{Guid.NewGuid():N}.db");
        var cs = $"Data Source={path}";
        using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions { ConnectionString = cs }));
        const string key = "idem-50-sqlite";
        var now = DateTimeOffset.UtcNow;
        var tasks = Enumerable.Range(0, 50).Select(_ => store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "c",
            ContractVersion = 1,
            Payload = "{}",
            IdempotencyKey = key,
            IdempotencyTtl = TimeSpan.FromHours(1),
        }, now, CancellationToken.None));
        var ids = await Task.WhenAll(tasks);
        Assert.Single(ids.Distinct());
        var counts = await store.GetCountsByStateAsync(CancellationToken.None);
        Assert.Equal(1, counts.GetValueOrDefault(JobState.Ready));
    }

    [Theory]
    [InlineData("postgres", "DOTNETJOBKIT_PG_CONNECTION")]
    [InlineData("mysql", "DOTNETJOBKIT_MYSQL_CONNECTION")]
    public async Task Provider_fifty_way_idempotency(string provider, string envVar)
    {
        var cs = Environment.GetEnvironmentVariable(envVar);
        if (string.IsNullOrWhiteSpace(cs))
            return;

        IJobStore store = provider switch
        {
            "postgres" => new PostgreSqlJobStore(Options.Create(new PostgreSqlJobStoreOptions { ConnectionString = cs })),
            "mysql" => new MySqlJobStore(Options.Create(new MySqlJobStoreOptions { ConnectionString = cs })),
            _ => throw new InvalidOperationException(),
        };

        using (store as IDisposable)
        {
            var key = $"idem-50-{provider}";
            var now = DateTimeOffset.UtcNow;
            var tasks = Enumerable.Range(0, 50).Select(_ => store.SubmitAsync(new JobSubmitRequest
            {
                Queue = "default",
                ContractName = "c",
                ContractVersion = 1,
                Payload = "{}",
                IdempotencyKey = key,
                IdempotencyTtl = TimeSpan.FromHours(1),
            }, now, CancellationToken.None));
            var ids = await Task.WhenAll(tasks);
            var distinct = ids.Distinct().ToList();
            Assert.Single(distinct);
            var job = await store.GetAsync(distinct[0], CancellationToken.None);
            Assert.NotNull(job);
            Assert.Equal(key, job!.IdempotencyKey);
            Assert.True(job.State is JobState.Ready or JobState.Leased);
        }
    }
}
