namespace DotnetJobKit.Abstractions;

public enum JobState : byte
{
    Ready = 0,
    Leased = 1,
    Succeeded = 2,
    Dead = 3,
    Cancelled = 4,
}
