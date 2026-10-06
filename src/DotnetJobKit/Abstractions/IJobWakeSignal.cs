namespace DotnetJobKit.Abstractions;

public interface IJobWakeSignal
{
    void Notify();

    Task WaitUntilAsync(DateTimeOffset wakeAt, CancellationToken cancellationToken);

    Task WaitForSignalAsync(CancellationToken cancellationToken);
}
