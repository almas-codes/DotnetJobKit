using System.Threading.Channels;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.Benchmarks;

public sealed class ChannelBaselineWorker : BackgroundService
{
    private readonly Channel<BenchmarkWorkItem> _channel;
    private int _completed;

    public ChannelBaselineWorker(Channel<BenchmarkWorkItem> channel)
    {
        _channel = channel;
    }

    public int Completed => Volatile.Read(ref _completed);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var item in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            item.Execute();
            Interlocked.Increment(ref _completed);
        }
    }
}

public readonly struct BenchmarkWorkItem(byte[] payload)
{
    public byte[] Payload { get; } = payload;

    public void Execute()
    {
        _ = Payload.Length;
    }
}
