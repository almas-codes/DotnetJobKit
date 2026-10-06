namespace DotnetJobKit.Abstractions;

public sealed class JobRetentionPurge
{
    public TimeSpan SucceededRetention { get; init; }
    public TimeSpan FailedRetention { get; init; }
    public TimeSpan CancelledRetention { get; init; }
}
