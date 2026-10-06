namespace DotnetJobKit.Runtime;

public static class RetryDelayCalculator
{
    public static TimeSpan ComputeDelay(
        Configuration.RetryPolicyOptions policy,
        int attemptCount,
        Random? random = null)
    {
        random ??= Random.Shared;

        if (policy.Kind == Configuration.RetryPolicyKind.NoRetry)
            return TimeSpan.Zero;

        var exponent = Math.Max(0, attemptCount - 1);
        var baseDelay = policy.Kind switch
        {
            Configuration.RetryPolicyKind.Fixed => policy.InitialDelay,
            Configuration.RetryPolicyKind.Exponential => TimeSpan.FromMilliseconds(
                policy.InitialDelay.TotalMilliseconds * Math.Pow(2, exponent)),
            _ => policy.InitialDelay,
        };

        if (baseDelay > policy.MaxDelay)
            baseDelay = policy.MaxDelay;

        if (policy.JitterRatio <= 0)
            return baseDelay;

        var jitterMs = baseDelay.TotalMilliseconds * policy.JitterRatio;
        var offset = (random.NextDouble() * 2 - 1) * jitterMs;
        var totalMs = Math.Max(0, baseDelay.TotalMilliseconds + offset);
        return TimeSpan.FromMilliseconds(totalMs);
    }
}
