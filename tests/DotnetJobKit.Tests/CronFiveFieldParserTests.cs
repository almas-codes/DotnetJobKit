using DotnetJobKit.Scheduling;

namespace DotnetJobKit.Tests;

public class CronFiveFieldParserTests
{
    [Fact]
    public void Every_minute_cron_advances()
    {
        var start = new DateTimeOffset(2026, 1, 1, 10, 15, 30, TimeSpan.Zero);
        var next = CronFiveFieldParser.GetNextOccurrence("* * * * *", start);
        Assert.Equal(new DateTimeOffset(2026, 1, 1, 10, 16, 0, TimeSpan.Zero), next);
    }
}
