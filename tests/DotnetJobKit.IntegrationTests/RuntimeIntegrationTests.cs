using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.IntegrationTests;

public sealed record PingJob(string Message);

public sealed class PingHandler : IJobHandler<PingJob>
{
    public static int Completed;

    public ValueTask HandleAsync(PingJob job, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Completed);
        return ValueTask.CompletedTask;
    }
}

public class RuntimeIntegrationTests
{
    [Fact]
    public async Task Runtime_executes_enqueued_job()
    {
        PingHandler.Completed = 0;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobKit(options =>
        {
            options.MaxConcurrency = 2;
            options.Queues = ["default"];
            options.Retention.DeleteOnSuccess = true;
        });
        builder.Services.AddDotnetJobKitInMemoryStorage();
        builder.Services.AddDotnetJobHandler<PingJob, PingHandler>("tests.ping", "default");

        using var host = builder.Build();
        await host.StartAsync();

        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        await submitter.EnqueueAsync(new PingJob("hello"));

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (PingHandler.Completed == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        await host.StopAsync();
        Assert.Equal(1, PingHandler.Completed);
    }
}
