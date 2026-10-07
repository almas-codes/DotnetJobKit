using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.IntegrationTests;

public sealed record NoOpJob;

public sealed class NoOpHandler : IJobHandler<NoOpJob>
{
    public static int Completed;
    public Task HandleAsync(NoOpJob job, JobContext context, CancellationToken cancellationToken)
        => Task.CompletedTask;
}

public class ExecutionKernelCorrectnessTests
{
    [Fact]
    public async Task Stale_attempt_cannot_commit_outcome()
    {
        var store = new InMemoryJobStore();
        var now = DateTimeOffset.UtcNow;
        var jobId = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "test",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        var claimedA = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None);
        var attemptA = claimedA[0].AttemptCount;
        var tokenA = claimedA[0].LeaseToken;

        _ = await store.ClaimAsync(["default"], 1, now.AddMinutes(2), TimeSpan.FromMinutes(1), CancellationToken.None);

        var stale = await store.CommitOutcomeAsync(
            new CommitOutcomeRequest(
                jobId,
                attemptA,
                tokenA,
                new ExecutionOutcome(ExecutionOutcomeKind.Succeeded),
                3,
                new RetryPolicyOptions()),
            now.AddMinutes(3),
            CancellationToken.None);

        Assert.Equal(CommitOutcomeStatus.StaleOwner, stale.Status);
    }

    [Fact]
    public async Task Duplicate_commit_outcome_is_idempotent()
    {
        var store = new InMemoryJobStore();
        var now = DateTimeOffset.UtcNow;
        var jobId = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "test",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        var claimed = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None);
        var job = claimed[0];
        var cont = new ContinuationSpec("default", "child", 1, "{}");
        var outcome = new ExecutionOutcome(ExecutionOutcomeKind.Succeeded, Continuation: cont);
        var commit = new CommitOutcomeRequest(
            job.JobId,
            job.AttemptCount,
            job.LeaseToken,
            outcome,
            job.MaxAttempts,
            new RetryPolicyOptions());

        var first = await store.CommitOutcomeAsync(commit, now, CancellationToken.None);
        var second = await store.CommitOutcomeAsync(commit, now, CancellationToken.None);

        Assert.Equal(CommitOutcomeStatus.Committed, first.Status);
        Assert.Equal(CommitOutcomeStatus.AlreadyCommitted, second.Status);
        Assert.Equal(first.SuccessorJobId, second.SuccessorJobId);
    }

    [Fact]
    public async Task Fifty_concurrent_submits_with_same_idempotency_key_create_one_job()
    {
        var store = new InMemoryJobStore();
        var now = DateTimeOffset.UtcNow;
        const string key = "idem-50";
        var tasks = Enumerable.Range(0, 50).Select(_ => store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "test",
            ContractVersion = 1,
            Payload = "{}",
            IdempotencyKey = key,
            IdempotencyTtl = TimeSpan.FromHours(1),
        }, now, CancellationToken.None));

        var ids = await Task.WhenAll(tasks);
        Assert.Single(ids.Distinct());
    }

    [Fact]
    public async Task Runtime_respects_max_concurrency_slots()
    {
        NoOpHandler.Completed = 0;
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobKit(options =>
        {
            options.MaxConcurrency = 2;
            options.Queues = ["default"];
            options.Retention.DeleteOnSuccess = true;
        });
        builder.Services.AddDotnetJobKitInMemoryStorage();
        builder.Services.AddDotnetJobHandler<NoOpJob, NoOpHandler>("noop", "default");

        using var host = builder.Build();
        await host.StartAsync();
        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        for (var i = 0; i < 10; i++)
            await submitter.EnqueueAsync(new NoOpJob());

        var store = host.Services.GetRequiredService<IJobStore>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var counts = await store.GetCountsByStateAsync(CancellationToken.None);
            var pending = counts.GetValueOrDefault(JobState.Ready) + counts.GetValueOrDefault(JobState.Leased);
            if (pending == 0)
                break;
            await Task.Delay(100);
        }

        await host.StopAsync();
        var finalCounts = await store.GetCountsByStateAsync(CancellationToken.None);
        Assert.Equal(0, finalCounts.GetValueOrDefault(JobState.Ready));
        Assert.Equal(0, finalCounts.GetValueOrDefault(JobState.Leased));
    }
}
