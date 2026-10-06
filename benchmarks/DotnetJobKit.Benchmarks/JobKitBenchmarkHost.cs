using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.Benchmarks;

public sealed record BenchJob(byte[] Payload);

public sealed class BenchJobHandler : IJobHandler<BenchJob>
{
    public static int Completed;

    public ValueTask HandleAsync(BenchJob job, JobContext context, CancellationToken cancellationToken)
    {
        _ = job.Payload.Length;
        Interlocked.Increment(ref Completed);
        return ValueTask.CompletedTask;
    }
}

public static class JobKitBenchmarkHost
{
    public static IHost CreateInMemory(int maxConcurrency, int payloadBytes)
    {
        BenchJobHandler.Completed = 0;
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDotnetJobKit(o =>
                {
                    o.MaxConcurrency = maxConcurrency;
                    o.Queues = ["default"];
                    o.Retention.DeleteOnSuccess = true;
                    o.CancellationPollInterval = TimeSpan.FromSeconds(1);
                });
                services.AddDotnetJobKitInMemoryStorage();
                services.AddDotnetJobHandler<BenchJob, BenchJobHandler>("bench.job", "default");
            })
            .Build();
    }

    public static async Task EnqueueBatchAsync(IJobSubmitter submitter, int count, int payloadBytes, CancellationToken cancellationToken)
    {
        var payload = new byte[payloadBytes];
        for (var i = 0; i < count; i++)
        {
            await submitter.EnqueueAsync(new BenchJob(payload), cancellationToken).ConfigureAwait(false);
        }
    }
}
