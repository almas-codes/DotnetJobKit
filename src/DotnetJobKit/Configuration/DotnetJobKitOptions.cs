namespace DotnetJobKit.Configuration;

public sealed class DotnetJobKitOptions
{
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;
    public string[] Queues { get; set; } = ["default"];
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(2);
    public TimeSpan LeaseRenewInterval { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan ExecutionTimeout { get; set; } = TimeSpan.FromMinutes(10);
    public TimeoutBehavior TimeoutBehavior { get; set; } = TimeoutBehavior.Cooperative;
    public TimeSpan MaxIdleSleep { get; set; } = TimeSpan.FromHours(12);
    public TimeSpan ReconciliationInterval { get; set; } = TimeSpan.FromSeconds(5);
    public int ErrorBackoffSeconds { get; set; } = 30;
    public int PayloadMaxBytes { get; set; } = 64 * 1024;
    public RetryPolicyOptions DefaultRetry { get; set; } = new();
    public RetentionOptions Retention { get; set; } = new();
    public OwnershipFencingMode OwnershipFencing { get; set; } = OwnershipFencingMode.AttemptCount;
    public TimeSpan CancellationPollInterval { get; set; } = TimeSpan.FromSeconds(1);
}
