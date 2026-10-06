using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.IntegrationTests;

public sealed record SlowJob(int DelayMs);

public sealed class SlowJobHandler : IJobHandler<SlowJob>
{
    public static int Cancelled;

    public async ValueTask HandleAsync(SlowJob job, JobContext context, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(job.DelayMs, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Interlocked.Increment(ref Cancelled);
            throw;
        }
    }
}

public class CancellationPollingTests
{
    [Fact]
    public async Task Leased_cancel_stops_long_handler_via_poll()
    {
        SlowJobHandler.Cancelled = 0;

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDotnetJobKit(o =>
        {
            o.MaxConcurrency = 1;
            o.CancellationPollInterval = TimeSpan.FromMilliseconds(200);
            o.ExecutionTimeout = TimeSpan.FromMinutes(5);
        });
        builder.Services.AddDotnetJobKitInMemoryStorage();
        builder.Services.AddDotnetJobHandler<SlowJob, SlowJobHandler>("tests.slow", "default");

        using var host = builder.Build();
        await host.StartAsync();

        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        var id = await submitter.EnqueueAsync(new SlowJob(10_000));

        await Task.Delay(300);
        await submitter.CancelAsync(id);

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (SlowJobHandler.Cancelled == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(100);

        await host.StopAsync();
        Assert.Equal(1, SlowJobHandler.Cancelled);
    }
}
