namespace DotnetJobKit.Sqlite;

internal static class SqliteSchema
{
    public const string CreateJobsTable = """
        CREATE TABLE IF NOT EXISTS Jobs (
            JobId TEXT NOT NULL PRIMARY KEY,
            Queue TEXT NOT NULL,
            ContractName TEXT NOT NULL,
            ContractVersion INTEGER NOT NULL,
            Payload TEXT NOT NULL,
            State INTEGER NOT NULL,
            EligibleAt TEXT NULL,
            AttemptCount INTEGER NOT NULL,
            MaxAttempts INTEGER NOT NULL,
            CancellationRequested INTEGER NOT NULL,
            CreatedAt TEXT NOT NULL,
            CompletedAt TEXT NULL,
            LastError TEXT NULL,
            IdempotencyKey TEXT NULL,
            IdempotencyExpiresAt TEXT NULL,
            LeaseToken TEXT NULL
        );
        """;

    public const string CreateDispatchIndex = """
        CREATE INDEX IF NOT EXISTS IX_Jobs_Dispatch
        ON Jobs (Queue, EligibleAt, JobId)
        WHERE State IN (0, 1);
        """;

    public const string CreateIdempotencyIndex = """
        CREATE UNIQUE INDEX IF NOT EXISTS IX_Jobs_ActiveIdempotencyKey
        ON Jobs (IdempotencyKey)
        WHERE IdempotencyKey IS NOT NULL AND State IN (0, 1);
        """;
}
