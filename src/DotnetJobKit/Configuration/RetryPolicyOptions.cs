namespace DotnetJobKit.Configuration;

public sealed class RetryPolicyOptions
{
    public RetryPolicyKind Kind { get; set; } = RetryPolicyKind.Exponential;
    public int MaxAttempts { get; set; } = 3;
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromHours(1);
    public double JitterRatio { get; set; } = 0.2;
}
