namespace DotnetJobKit.IntegrationTests;

internal static class StressTestSettings
{
    public static int MegaJobCount
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("DOTNET_JOBKIT_MEGA_JOBS");
            if (string.IsNullOrWhiteSpace(raw))
                return 100_000;

            return int.TryParse(raw, out var count) ? count : 100_000;
        }
    }

    public static bool RunMegaTenMillion =>
        string.Equals(Environment.GetEnvironmentVariable("DOTNET_JOBKIT_RUN_10M"), "1", StringComparison.Ordinal);
}
