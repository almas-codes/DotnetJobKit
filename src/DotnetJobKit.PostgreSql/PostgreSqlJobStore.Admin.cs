using DotnetJobKit.Abstractions;
using Npgsql;

namespace DotnetJobKit.PostgreSql;

public sealed partial class PostgreSqlJobStore
{
    public async Task<IReadOnlyDictionary<JobState, int>> GetCountsByStateAsync(CancellationToken cancellationToken)
    {
        var counts = Enum.GetValues<JobState>().ToDictionary(s => s, _ => 0);
        await using var cmd = new NpgsqlCommand("SELECT state, COUNT(*) FROM djk_jobs GROUP BY state;", _connection);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            counts[(JobState)reader.GetInt16(0)] = reader.GetInt32(1);
        return counts;
    }

    public async Task<IReadOnlyList<JobRecord>> ListJobsAsync(
        JobState? state,
        string? queue,
        int limit,
        CancellationToken cancellationToken)
    {
        var filters = new List<string>();
        await using var cmd = new NpgsqlCommand { Connection = _connection };
        if (state is not null)
        {
            filters.Add("state = @state");
            cmd.Parameters.AddWithValue("state", (short)state);
        }

        if (!string.IsNullOrWhiteSpace(queue))
        {
            filters.Add("queue = @queue");
            cmd.Parameters.AddWithValue("queue", queue);
        }

        var where = filters.Count == 0 ? string.Empty : "WHERE " + string.Join(" AND ", filters);
        cmd.CommandText = $"""
            SELECT * FROM djk_jobs {where}
            ORDER BY created_at DESC
            LIMIT @limit;
            """;
        cmd.Parameters.AddWithValue("limit", Math.Max(1, limit));
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var list = new List<JobRecord>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            list.Add(ReadRecord(reader));
        return list;
    }

    public async Task<bool> RequeueDeadAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            """
            UPDATE djk_jobs SET state = 0, eligible_at = @now, attempt_count = 0, completed_at = NULL,
                last_error = NULL, lease_token = NULL, cancellation_requested = false
            WHERE job_id = @job_id AND state = 3;
            """,
            _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("now", now);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> DeleteTerminalJobAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            "DELETE FROM djk_jobs WHERE job_id = @job_id AND state IN (2, 3, 4);",
            _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> CompleteAsRecurringReadyAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        DateTimeOffset nextEligibleAt,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            """
            UPDATE djk_jobs SET state = 0, attempt_count = 0, eligible_at = @next_eligible, completed_at = NULL,
                last_error = NULL, lease_token = NULL, cancellation_requested = false
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("lease_token", (object?)leaseToken ?? DBNull.Value);
        cmd.Parameters.AddWithValue("next_eligible", nextEligibleAt);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }
}
