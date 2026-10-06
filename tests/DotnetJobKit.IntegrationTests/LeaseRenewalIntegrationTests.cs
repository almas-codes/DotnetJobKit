using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.IntegrationTests;

public sealed record LongRunningJob(int Id);

public sealed class LongRunningJobHandler : IJobHandler<LongRunningJob>
{
    public static int ExecutionsStarted;
    public static int ExecutionsCompleted;
    public static ManualResetEventSlim? ReleaseGate;

    public async Task HandleAsync(LongRunningJob job, JobContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref ExecutionsStarted);
        var gate = ReleaseGate ?? throw new InvalidOperationException("ReleaseGate not set.");
        await Task.Run(() => gate.Wait(cancellationToken), cancellationToken).ConfigureAwait(false);
        Interlocked.Increment(ref ExecutionsCompleted);
    }
}

public class LeaseRenewalIntegrationTests
{
    [Fact]
    public async Task Long_handler_with_short_lease_is_not_double_executed_across_two_workers()
    {
        LongRunningJobHandler.ExecutionsStarted = 0;
        LongRunningJobHandler.ExecutionsCompleted = 0;
        using var gate = new ManualResetEventSlim(false);
        LongRunningJobHandler.ReleaseGate = gate;

        var dbPath = Path.Combine(Path.GetTempPath(), $"djk-lease-{Guid.NewGuid():N}.db");
        var cs = $"Data Source={dbPath}";

        void Configure(IHostApplicationBuilder b)
        {
            b.Services.AddDotnetJobKit(o =>
            {
                o.MaxConcurrency = 1;
                o.Queues = ["default"];
                o.LeaseDuration = TimeSpan.FromSeconds(2);
                o.LeaseRenewInterval = TimeSpan.FromMilliseconds(400);
                o.Retention.DeleteOnSuccess = true;
            });
            b.Services.AddDotnetJobKitSqlite(cs);
            b.Services.AddDotnetJobHandler<LongRunningJob, LongRunningJobHandler>("tests.long", "default");
        }

        var builder1 = Host.CreateApplicationBuilder();
        Configure(builder1);
        var builder2 = Host.CreateApplicationBuilder();
        Configure(builder2);

        using var host1 = builder1.Build();
        using var host2 = builder2.Build();
        await host1.StartAsync();
        await host2.StartAsync();

        var submitter = host1.Services.GetRequiredService<IJobSubmitter>();
        await submitter.EnqueueAsync(new LongRunningJob(1));

        await Task.Delay(TimeSpan.FromSeconds(6));

        Assert.Equal(1, LongRunningJobHandler.ExecutionsStarted);

        gate.Set();
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.Equal(1, LongRunningJobHandler.ExecutionsCompleted);

        await host2.StopAsync();
        await host1.StopAsync();
    }
}
