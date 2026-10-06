using DotnetJobKit.Abstractions;
using DotnetJobKit.Client;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.IntegrationTests;

public sealed record ChainParentJob(string Step);
public sealed record ChainChildJob(string Step);

public sealed class ChainParentHandler : IJobHandler<ChainParentJob>
{
    public static int ParentRuns;
    public Task HandleAsync(ChainParentJob job, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref ParentRuns);
        return Task.CompletedTask;
    }
}

public sealed class ChainChildHandler : IJobHandler<ChainChildJob>
{
    public static int ChildRuns;
    public Task HandleAsync(ChainChildJob job, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref ChildRuns);
        return Task.CompletedTask;
    }
}

public sealed record UniqueJob(string Key);

public sealed class UniqueJobHandler : IJobHandler<UniqueJob>
{
    public static int Runs;
    public Task HandleAsync(UniqueJob job, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Runs);
        return Task.CompletedTask;
    }
}

public class EnqueueFeatureTests
{
    [Fact]
    public async Task Chain_parent_handler_runs()
    {
        ChainParentHandler.ParentRuns = 0;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobHandler<ChainParentJob, ChainParentHandler>("chain.parent", "default");
        builder.Services.AddDotnetJobHandler<ChainChildJob, ChainChildHandler>("chain.child", "default");
        builder.Services.AddDotnetJobKit(o => o.MaxConcurrency = 2);
        builder.Services.AddDotnetJobKitInMemoryStorage();

        using var host = builder.Build();
        await host.StartAsync();
        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        await submitter.EnqueueAsync(new ChainParentJob("solo"));

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (ChainParentHandler.ParentRuns == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        await host.StopAsync();
        Assert.Equal(1, ChainParentHandler.ParentRuns);
    }

    [Fact]
    public async Task ContinueWith_persists_continuation_metadata()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobHandler<ChainParentJob, ChainParentHandler>("chain.parent", "default");
        builder.Services.AddDotnetJobHandler<ChainChildJob, ChainChildHandler>("chain.child", "default");
        builder.Services.AddDotnetJobKit();
        builder.Services.AddDotnetJobKitInMemoryStorage();

        using var host = builder.Build();
        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        var store = host.Services.GetRequiredService<IJobStore>();
        var parentId = await submitter.ContinueWithAsync(new ChainParentJob("a"), new ChainChildJob("b"));
        var meta = await store.GetAsync(parentId, CancellationToken.None);
        Assert.Equal("chain.child", meta!.ContinuationContractName);
        Assert.False(string.IsNullOrWhiteSpace(meta.ContinuationPayload));
    }

    [Fact]
    public async Task UniqueKey_skips_duplicate_while_pending()
    {
        UniqueJobHandler.Runs = 0;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobKit(o => o.MaxConcurrency = 1);
        builder.Services.AddDotnetJobKitInMemoryStorage();
        builder.Services.AddDotnetJobHandler<UniqueJob, UniqueJobHandler>("unique.job", "default");

        using var host = builder.Build();
        await host.StartAsync();
        var submitter = host.Services.GetRequiredService<IJobSubmitter>();

        var id1 = await submitter.EnqueueWithUniqueKeyAsync(new UniqueJob("x"), "key-1");
        var id2 = await submitter.EnqueueWithUniqueKeyAsync(new UniqueJob("x"), "key-1");
        Assert.Equal(id1, id2);

        await host.StopAsync();
    }
}
