using DotnetJobKit.Abstractions;
using DotnetJobKit.Client;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.IntegrationTests.Helpers;
using DotnetJobKit.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.IntegrationTests;

public sealed record ImmediateScenarioJob(string Tag);
public sealed record RecurringTickJob(int Sequence);

public sealed class ImmediateScenarioHandler : IJobHandler<ImmediateScenarioJob>
{
    public static int Runs;
    public Task HandleAsync(ImmediateScenarioJob job, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Runs);
        return Task.CompletedTask;
    }
}

public sealed class RecurringTickHandler : IJobHandler<RecurringTickJob>
{
    public static int Runs;

    public Task HandleAsync(RecurringTickJob job, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Runs);
        return Task.CompletedTask;
    }
}

public class JobScenarioTests
{
    [Fact]
    public async Task Immediate_enqueue_runs_once()
    {
        ImmediateScenarioHandler.Runs = 0;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobHandler<ImmediateScenarioJob, ImmediateScenarioHandler>("scenario.immediate", "default");
        builder.Services.AddDotnetJobKit(o => o.MaxConcurrency = 2);
        builder.Services.AddDotnetJobKitInMemoryStorage();

        using var host = builder.Build();
        await host.StartAsync();
        await host.Services.GetRequiredService<IJobSubmitter>().EnqueueAsync(new ImmediateScenarioJob("now"));

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (ImmediateScenarioHandler.Runs == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        await host.StopAsync();
        Assert.Equal(1, ImmediateScenarioHandler.Runs);
    }

    [Fact]
    public async Task Unique_key_dedupes_while_job_still_pending()
    {
        UniqueJobHandler.Runs = 0;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobHandler<UniqueJob, UniqueJobHandler>("unique.job", "default");
        builder.Services.AddDotnetJobKit(o => o.MaxConcurrency = 1);
        builder.Services.AddDotnetJobKitInMemoryStorage();

        using var host = builder.Build();
        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        var a = await submitter.EnqueueWithUniqueKeyAsync(new UniqueJob("a"), "dedupe-1");
        var b = await submitter.EnqueueWithUniqueKeyAsync(new UniqueJob("b"), "dedupe-1");
        Assert.Equal(a, b);

        await host.StartAsync();

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (UniqueJobHandler.Runs < 1 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        await host.StopAsync();
        Assert.Equal(1, UniqueJobHandler.Runs);
    }

    [Fact]
    public void Cron_parser_computes_next_minute_boundary()
    {
        var start = new DateTimeOffset(2026, 3, 15, 14, 30, 45, TimeSpan.Zero);
        var next = CronFiveFieldParser.GetNextOccurrence("* * * * *", start);
        Assert.Equal(new DateTimeOffset(2026, 3, 15, 14, 31, 0, TimeSpan.Zero), next);
    }

    [Fact]
    public async Task Recurring_job_returns_to_ready_after_success_with_fake_time()
    {
        RecurringTickHandler.Runs = 0;

        var time = new FakeTimeProvider(new DateTimeOffset(2026, 5, 1, 10, 0, 0, TimeSpan.Zero));
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<TimeProvider>(time);
        builder.Services.AddDotnetJobHandler<RecurringTickJob, RecurringTickHandler>("scenario.recurring", "default");
        builder.Services.AddDotnetJobKit(o =>
        {
            o.MaxConcurrency = 1;
            o.Retention.DeleteOnSuccess = false;
            o.LeaseRenewInterval = TimeSpan.Zero;
            o.CancellationPollInterval = TimeSpan.Zero;
        });
        builder.Services.AddDotnetJobKitInMemoryStorage();

        using var host = builder.Build();
        await host.StartAsync();

        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        var store = host.Services.GetRequiredService<IJobStore>();
        var wake = host.Services.GetRequiredService<IJobWakeSignal>();

        var jobId = await submitter.EnqueueRecurringAsync(new RecurringTickJob(1), "* * * * *");

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (RecurringTickHandler.Runs < 1 && DateTime.UtcNow < deadline)
            await Task.Delay(10);

        Assert.Equal(1, RecurringTickHandler.Runs);

        var afterFirst = await store.GetAsync(jobId, CancellationToken.None);
        Assert.NotNull(afterFirst);
        Assert.Equal("* * * * *", afterFirst!.RecurrenceCron);

        var nextRun = afterFirst.State == JobState.Ready && afterFirst.EligibleAt is { } readyAt
            ? readyAt
            : CronFiveFieldParser.GetNextOccurrence("* * * * *", time.GetUtcNow());
        if (afterFirst.State == JobState.Leased && afterFirst.EligibleAt is { } leaseEnd)
            nextRun = leaseEnd.AddSeconds(1);

        time.SetUtcNow(nextRun.AddSeconds(1));
        wake.Notify();

        deadline = DateTime.UtcNow.AddSeconds(10);
        while (RecurringTickHandler.Runs < 2 && DateTime.UtcNow < deadline)
            await Task.Delay(10);

        await host.StopAsync();
        Assert.Equal(2, RecurringTickHandler.Runs);
    }

    [Fact]
    public async Task ContinueWith_runs_child_after_parent_via_explicit_chain()
    {
        ChainParentHandler.ParentRuns = 0;
        ChainChildHandler.ChildRuns = 0;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobHandler<ChainParentJob, ChainParentHandler>("chain.parent", "default");
        builder.Services.AddDotnetJobHandler<ChainChildJob, ChainChildHandler>("chain.child", "default");
        builder.Services.AddDotnetJobKit(o =>
        {
            o.MaxConcurrency = 2;
            o.Retention.DeleteOnSuccess = true;
        });
        builder.Services.AddDotnetJobKitInMemoryStorage();

        using var host = builder.Build();
        await host.StartAsync();
        var submitter = host.Services.GetRequiredService<IJobSubmitter>();

        await submitter.EnqueueAsync(new ChainParentJob("p"));
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (ChainParentHandler.ParentRuns == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        await submitter.EnqueueAsync(new ChainChildJob("c"));
        deadline = DateTime.UtcNow.AddSeconds(5);
        while (ChainChildHandler.ChildRuns == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        await host.StopAsync();
        Assert.Equal(1, ChainParentHandler.ParentRuns);
        Assert.Equal(1, ChainChildHandler.ChildRuns);
    }
}
