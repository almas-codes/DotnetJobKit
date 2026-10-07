namespace DotnetJobKit.Diagnostics;

/// <summary>
/// Optional hooks for integration tests and benchmark instrumentation.
/// </summary>
public static class JobStoreDiagnostics
{
    public static int SqlCommandCount;

    public static void Reset() => SqlCommandCount = 0;

    public static void RecordCommand() => Interlocked.Increment(ref SqlCommandCount);
}
