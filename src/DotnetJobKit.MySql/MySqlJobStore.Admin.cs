using DotnetJobKit.Abstractions;
using MySqlConnector;

namespace DotnetJobKit.MySql;

public sealed partial class MySqlJobStore
{
    public async Task<IReadOnlyDictionary<JobState, int>> GetCountsByStateAsync(CancellationToken cancellationToken)
    {
        var counts = Enum.GetValues<JobState>().ToDictionary(s => s, _ => 0);
        await using var cmd = new MySqlCommand("SELECT state, COUNT(*) FROM djk_jobs GROUP BY state;", _connection);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            counts[(JobState)reader.GetByte(0)] = reader.GetInt32(1);
        return counts;
    }

    public async Task<IReadOnlyList<JobRecord>> ListJobsAsync(
        JobState? state,
        string? queue,
        int limit,
        CancellationToken cancellationToken)
    {
        var filters = new List<string>();
        await using var cmd = new MySqlCommand { Connection = _connection };
        if (state is not null)
        {
            filters.Add("state = @state");
            cmd.Parameters.AddWithValue("@state", (byte)state);
        }

        if (!string.IsNullOrWhiteSpace(queue))
        {
            filters.Add("queue = @queue");
            cmd.Parameters.AddWithValue("@queue", queue);
        }

        var where = filters.Count == 0 ? string.Empty : "WHERE " + string.Join(" AND ", filters);
        cmd.CommandText = $"""
            SELECT * FROM djk_jobs {where}
            ORDER BY created_at DESC
            LIMIT @limit;
            """;
        cmd.Parameters.AddWithValue("@limit", Math.Max(1, limit));
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var list = new List<JobRecord>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            list.Add(ReadRecord(reader));
        return list;
    }

    public async Task<bool> RequeueDeadAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            """
            UPDATE djk_jobs SET state = 0, eligible_at = @now, attempt_count = 0, completed_at = NULL,
                last_error = NULL, lease_token = NULL, cancellation_requested = 0
            WHERE job_id = @job_id AND state = 3;
            """,
            _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@now", now.UtcDateTime);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> DeleteTerminalJobAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            "DELETE FROM djk_jobs WHERE job_id = @job_id AND state IN (2, 3, 4);",
            _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
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
        await using var cmd = new MySqlCommand(
            """
            UPDATE djk_jobs SET state = 0, attempt_count = 0, eligible_at = @next_eligible, completed_at = NULL,
                last_error = NULL, lease_token = NULL, cancellation_requested = 0
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("@lease_token", leaseToken?.ToString() ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@next_eligible", nextEligibleAt.UtcDateTime);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private static JobRecord ReadRecord(MySqlDataReader reader) => new()
    {
        JobId = Guid.Parse(reader.GetString("job_id")),
        Queue = reader.GetString("queue"),
        ContractName = reader.GetString("contract_name"),
        ContractVersion = reader.GetInt32("contract_version"),
        Payload = reader.GetString("payload"),
        State = (JobState)reader.GetByte("state"),
        EligibleAt = reader.IsDBNull(reader.GetOrdinal("eligible_at"))
            ? null
            : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime("eligible_at"), DateTimeKind.Utc)),
        AttemptCount = reader.GetInt32("attempt_count"),
        MaxAttempts = reader.GetInt32("max_attempts"),
        CancellationRequested = reader.GetBoolean("cancellation_requested"),
        CreatedAt = new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime("created_at"), DateTimeKind.Utc)),
        CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at"))
            ? null
            : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime("completed_at"), DateTimeKind.Utc)),
        LastError = reader.IsDBNull(reader.GetOrdinal("last_error")) ? null : reader.GetString("last_error"),
        IdempotencyKey = reader.IsDBNull(reader.GetOrdinal("idempotency_key")) ? null : reader.GetString("idempotency_key"),
        RecurrenceCron = TryGetString(reader, "recurrence_cron"),
        ContinuationQueue = TryGetString(reader, "continuation_queue"),
        ContinuationContractName = TryGetString(reader, "continuation_contract_name"),
        ContinuationContractVersion = TryGetInt(reader, "continuation_contract_version"),
        ContinuationPayload = TryGetString(reader, "continuation_payload"),
    };

    private static string? TryGetString(MySqlDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static int? TryGetInt(MySqlDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);
    }
}
