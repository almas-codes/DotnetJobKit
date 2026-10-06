namespace DotnetJobKit.Scheduling;

/// <summary>Minimal UTC cron parser (minute hour day month day-of-week). Supports *, numbers, and */n steps.</summary>
public static class CronFiveFieldParser
{
    public static DateTimeOffset GetNextOccurrence(string cronExpression, DateTimeOffset afterUtc)
    {
        var parts = cronExpression.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length != 5)
            throw new FormatException("Cron expression must have five fields: minute hour day month day-of-week.");

        var minute = ParseField(parts[0], 0, 59);
        var hour = ParseField(parts[1], 0, 23);
        var day = ParseField(parts[2], 1, 31);
        var month = ParseField(parts[3], 1, 12);
        var dow = ParseField(parts[4], 0, 6);

        var cursor = afterUtc.AddSeconds(-afterUtc.Second).AddMilliseconds(-afterUtc.Millisecond);
        if (cursor <= afterUtc)
            cursor = cursor.AddMinutes(1);

        for (var i = 0; i < 525_600; i++)
        {
            if (month.Contains(cursor.Month)
                && day.Contains(cursor.Day)
                && dow.Contains((int)cursor.DayOfWeek)
                && hour.Contains(cursor.Hour)
                && minute.Contains(cursor.Minute))
            {
                return cursor;
            }

            cursor = cursor.AddMinutes(1);
        }

        throw new InvalidOperationException($"Could not find next occurrence for cron '{cronExpression}'.");
    }

    private static HashSet<int> ParseField(string field, int min, int max)
    {
        if (field == "*")
            return Enumerable.Range(min, max - min + 1).ToHashSet();

        if (field.StartsWith("*/", StringComparison.Ordinal))
        {
            var step = int.Parse(field[2..]);
            var set = new HashSet<int>();
            for (var v = min; v <= max; v += step)
                set.Add(v);
            return set;
        }

        if (field.Contains(',', StringComparison.Ordinal))
        {
            var set = new HashSet<int>();
            foreach (var part in field.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                set.Add(int.Parse(part));
            return set;
        }

        return [int.Parse(field)];
    }
}
