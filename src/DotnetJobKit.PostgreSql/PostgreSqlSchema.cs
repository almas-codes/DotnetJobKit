namespace DotnetJobKit.PostgreSql;

internal static class PostgreSqlSchema
{
    public const string TableName = "djk_jobs";

    public const string CreateTable = """
        CREATE TABLE IF NOT EXISTS djk_jobs (
            job_id UUID PRIMARY KEY,
            queue TEXT NOT NULL,
            contract_name TEXT NOT NULL,
            contract_version INT NOT NULL,
            payload TEXT NOT NULL,
            state SMALLINT NOT NULL,
            eligible_at TIMESTAMPTZ NULL,
            attempt_count INT NOT NULL,
            max_attempts INT NOT NULL,
            cancellation_requested BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ NULL,
            last_error TEXT NULL,
            idempotency_key TEXT NULL,
            idempotency_expires_at TIMESTAMPTZ NULL,
            lease_token UUID NULL
        );
        """;

    public const string CreateDispatchIndex = """
        CREATE INDEX IF NOT EXISTS ix_djk_jobs_dispatch
        ON djk_jobs (queue, eligible_at, job_id)
        WHERE state IN (0, 1);
        """;
}
