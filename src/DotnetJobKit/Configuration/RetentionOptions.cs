namespace DotnetJobKit.Configuration;

public sealed class RetentionOptions
{
    public bool DeleteOnSuccess { get; set; } = true;
    public TimeSpan FailedRetention { get; set; } = TimeSpan.FromDays(30);
    public TimeSpan CancelledRetention { get; set; } = TimeSpan.FromDays(7);
}
