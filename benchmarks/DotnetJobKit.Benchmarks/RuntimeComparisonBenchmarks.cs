using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using DotnetJobKit.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net10_0, warmupCount: 1, iterationCount: 3)]
public class RuntimeComparisonBenchmarks
{
    private IHost? _jobKitHost;
    private IJobSubmitter? _submitter;
    private Channel<BenchmarkWorkItem>? _channel;
    private ChannelBaselineWorker? _channelWorkerHost;
    private IHost? _channelHost;

    [Params(256, 1024, 4096)]
    public int PayloadBytes { get; set; }

    [Params(100, 1000)]
    public int JobCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _jobKitHost = JobKitBenchmarkHost.CreateInMemory(maxConcurrency: 4, PayloadBytes);
        _jobKitHost.Start();
        _submitter = _jobKitHost.Services.GetRequiredService<IJobSubmitter>();

        _channel = Channel.CreateBounded<BenchmarkWorkItem>(new BoundedChannelOptions(10_000)
        {
            FullMode = BoundedChannelFullMode.Wait,
        });

        _channelHost = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(_channel);
                services.AddSingleton<ChannelBaselineWorker>();
                services.AddHostedService(sp => sp.GetRequiredService<ChannelBaselineWorker>());
            })
            .Build();
        _channelHost.Start();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _jobKitHost?.StopAsync().GetAwaiter().GetResult();
        _jobKitHost?.Dispose();
        _channelHost?.StopAsync().GetAwaiter().GetResult();
        _channelHost?.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task Channel_EnqueueAndProcess()
    {
        var worker = _channelHost!.Services.GetRequiredService<ChannelBaselineWorker>();
        var start = worker.Completed;
        var payload = new byte[PayloadBytes];
        for (var i = 0; i < JobCount; i++)
            await _channel!.Writer.WriteAsync(new BenchmarkWorkItem(payload)).ConfigureAwait(false);

        var deadline = Environment.TickCount64 + 30_000;
        while (worker.Completed - start < JobCount && Environment.TickCount64 < deadline)
            await Task.Delay(10).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task DotnetJobKit_InMemory_EnqueueAndProcess()
    {
        BenchJobHandler.Completed = 0;
        await JobKitBenchmarkHost.EnqueueBatchAsync(_submitter!, JobCount, PayloadBytes, CancellationToken.None)
            .ConfigureAwait(false);

        var deadline = Environment.TickCount64 + 30_000;
        while (BenchJobHandler.Completed < JobCount && Environment.TickCount64 < deadline)
            await Task.Delay(10).ConfigureAwait(false);
    }
}
