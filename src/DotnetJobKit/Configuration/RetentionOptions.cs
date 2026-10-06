namespace DotnetJobKit.Configuration;

public sealed class RetentionOptions
{
    /// <summary>When true, succeeded jobs are removed at settle time (legacy). Prefer <see cref="SucceededRetention"/>.</summary>
    public bool DeleteOnSuccess { get; set; }

    public TimeSpan SucceededRetention { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan FailedRetention { get; set; } = TimeSpan.FromDays(30);
    public TimeSpan CancelledRetention { get; set; } = TimeSpan.FromDays(7);
    public int PurgeBatchSize { get; set; } = 1000;

    public bool ShouldDeleteImmediatelyOnSuccess() =>
        DeleteOnSuccess || SucceededRetention <= TimeSpan.Zero;
}
