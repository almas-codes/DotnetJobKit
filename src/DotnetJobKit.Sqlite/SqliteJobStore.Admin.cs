using DotnetJobKit.Abstractions;

namespace DotnetJobKit.Sqlite;

public sealed partial class SqliteJobStore
{
    public async Task<IReadOnlyDictionary<JobState, int>> GetCountsByStateAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var counts = Enum.GetValues<JobState>().ToDictionary(s => s, _ => 0);
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT State, COUNT(*) FROM Jobs GROUP BY State;";
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                counts[(JobState)reader.GetInt32(0)] = reader.GetInt32(1);
            }

            return counts;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<JobRecord>> ListJobsAsync(
        JobState? state,
        string? queue,
        int limit,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var filters = new List<string>();
            await using var cmd = _connection.CreateCommand();
            if (state is not null)
            {
                filters.Add("State = $state");
                cmd.Parameters.AddWithValue("$state", (int)state);
            }

            if (!string.IsNullOrWhiteSpace(queue))
            {
                filters.Add("Queue = $queue");
                cmd.Parameters.AddWithValue("$queue", queue);
            }

            var where = filters.Count == 0 ? string.Empty : "WHERE " + string.Join(" AND ", filters);
            cmd.CommandText = $"""
                SELECT * FROM Jobs {where}
                ORDER BY CreatedAt DESC
                LIMIT $limit;
                """;
            cmd.Parameters.AddWithValue("$limit", Math.Max(1, limit));
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var list = new List<JobRecord>();
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                list.Add(ReadJobRecord(reader));

            return list;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> RequeueDeadAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                UPDATE Jobs SET State = 0, EligibleAt = $now, AttemptCount = 0, CompletedAt = NULL,
                    LastError = NULL, LeaseToken = NULL, CancellationRequested = 0
                WHERE JobId = $jobId AND State = 3;
                """;
            cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
            cmd.Parameters.AddWithValue("$now", now.ToString("O"));
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> DeleteTerminalJobAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "DELETE FROM Jobs WHERE JobId = $jobId AND State IN (2, 3, 4);";
            cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> CompleteAsRecurringReadyAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        DateTimeOffset nextEligibleAt,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                UPDATE Jobs SET State = 0, AttemptCount = 0, EligibleAt = $nextEligible, CompletedAt = NULL,
                    LastError = NULL, LeaseToken = NULL, CancellationRequested = 0
                WHERE JobId = $jobId AND State = 1 AND AttemptCount = $attemptCount
                  AND ($leaseToken IS NULL OR LeaseToken = $leaseToken);
                """;
            cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
            cmd.Parameters.AddWithValue("$attemptCount", attemptCount);
            cmd.Parameters.AddWithValue("$leaseToken", leaseToken?.ToString() ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("$nextEligible", nextEligibleAt.ToString("O"));
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }
        finally
        {
            _gate.Release();
        }
    }
}
