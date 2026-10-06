namespace DotnetJobKit.Configuration;

public enum RetryPolicyKind
{
    NoRetry = 0,
    Fixed = 1,
    Exponential = 2,
}
