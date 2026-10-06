using DotnetJobKit.Abstractions;

namespace DotnetJobKit.Runtime;

public sealed class JobWakeSignal : IJobWakeSignal
{
    private readonly TimeProvider _timeProvider;
    private readonly object _sync = new();
    private CancellationTokenSource _wakeCts = new();

    public JobWakeSignal(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
    }

    public void Notify()
    {
        lock (_sync)
        {
            if (!_wakeCts.IsCancellationRequested)
                _wakeCts.Cancel();
        }
    }

    public async Task WaitUntilAsync(DateTimeOffset wakeAt, CancellationToken cancellationToken)
    {
        var wakeToken = GetWakeToken();
        var delay = wakeAt - _timeProvider.GetUtcNow();
        if (delay <= TimeSpan.Zero)
            return;

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, wakeToken);
        try
        {
            await Task.Delay(delay, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            ResetWakeSourceIfCancelled();
        }
    }

    public async Task WaitForSignalAsync(CancellationToken cancellationToken)
    {
        var wakeToken = GetWakeToken();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, wakeToken);
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            ResetWakeSourceIfCancelled();
        }
    }

    private CancellationToken GetWakeToken()
    {
        lock (_sync)
        {
            ResetWakeSourceIfCancelled();
            return _wakeCts.Token;
        }
    }

    private void ResetWakeSourceIfCancelled()
    {
        if (!_wakeCts.IsCancellationRequested)
            return;

        _wakeCts.Dispose();
        _wakeCts = new CancellationTokenSource();
    }
}
