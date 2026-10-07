using DotnetJobKit.PostgreSql;
using Npgsql;

var cs = Environment.GetEnvironmentVariable("DOTNETJOBKIT_PG_CONNECTION")
    ?? throw new InvalidOperationException("Set DOTNETJOBKIT_PG_CONNECTION");
var jobs = ParseInt("--jobs", 100_000);
var outDir = Path.Combine(FindRepoRoot(), "docs", "benchmarks", "postgres-explain");
Directory.CreateDirectory(outDir);

await using var connection = new NpgsqlConnection(cs);
await connection.OpenAsync();
PostgreSqlJobStore.EnsureSchema(connection);

await using (var truncate = connection.CreateCommand())
{
    truncate.CommandText = "TRUNCATE djk_jobs;";
    await truncate.ExecuteNonQueryAsync();
}

await PostgreSqlJobBulkInserter.InsertReadyJobsAsync(
    connection, jobs, "default", "perf.job", 1, "{}", DateTimeOffset.UtcNow, 3, batchSize: 5_000);

var claimSql = """
    WITH picked AS (
        SELECT job_id, queue, contract_name, contract_version, payload, attempt_count, max_attempts, cancellation_requested
        FROM djk_jobs
        WHERE queue = ANY(@queues)
          AND state IN (0, 1)
          AND eligible_at IS NOT NULL
          AND eligible_at <= @now
          AND NOT (state = 1 AND attempt_count >= max_attempts)
        ORDER BY eligible_at, job_id
        FOR UPDATE SKIP LOCKED
        LIMIT @max_count
    )
    UPDATE djk_jobs AS j
    SET state = 1,
        attempt_count = picked.attempt_count + 1,
        eligible_at = @lease_expires,
        lease_token = CASE WHEN @use_token THEN gen_random_uuid() ELSE NULL END
    FROM picked
    WHERE j.job_id = picked.job_id
    RETURNING j.job_id;
    """;

await RunExplainAsync(connection, claimSql, outDir, $"claim-ready-{jobs}.txt", new Dictionary<string, object?>
{
    ["queues"] = new[] { "default" },
    ["now"] = DateTimeOffset.UtcNow,
    ["max_count"] = 16,
    ["lease_expires"] = DateTimeOffset.UtcNow.AddMinutes(1),
    ["use_token"] = false,
});

Console.WriteLine($"Wrote EXPLAIN output to {outDir}");

static async Task RunExplainAsync(
    NpgsqlConnection connection,
    string sql,
    string outDir,
    string fileName,
    Dictionary<string, object?> parameters)
{
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $"EXPLAIN (ANALYZE, BUFFERS, WAL) {sql}";
    foreach (var (key, value) in parameters)
        cmd.Parameters.AddWithValue(key, value ?? DBNull.Value);
    await using var reader = await cmd.ExecuteReaderAsync();
    var lines = new List<string>();
    while (await reader.ReadAsync())
        lines.Add(reader.GetString(0));
    await File.WriteAllLinesAsync(Path.Combine(outDir, fileName), lines);
}

static int ParseInt(string flag, int defaultValue)
{
    var arg = Environment.GetCommandLineArgs().Skip(1).FirstOrDefault(a => a.StartsWith(flag + "=", StringComparison.OrdinalIgnoreCase));
    return arg is null ? defaultValue : int.Parse(arg[(flag.Length + 1)..]);
}

static string FindRepoRoot()
{
    var dir = AppContext.BaseDirectory;
    while (!string.IsNullOrEmpty(dir))
    {
        if (Directory.Exists(Path.Combine(dir, "docs")) && Directory.Exists(Path.Combine(dir, "src")))
            return dir;
        dir = Directory.GetParent(dir)?.FullName ?? string.Empty;
    }

    return Directory.GetCurrentDirectory();
}
