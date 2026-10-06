namespace DotnetJobKit.Abstractions;

public enum SettleOutcome : byte
{
    Succeeded = 0,
    Retry = 1,
    Dead = 2,
    Cancelled = 3,
}
