namespace DotnetJobKit.Client;

public sealed class JobEnqueueOptions
{
    public string? UniqueKey { get; init; }
    public TimeSpan? IdempotencyTtl { get; init; }
    public string? RecurrenceCron { get; init; }
}
