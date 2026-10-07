namespace DotnetJobKit.MySql;

internal static class MySqlSchema
{
    public const string TableName = "djk_jobs";

    public const string CreateTable = """
        CREATE TABLE IF NOT EXISTS djk_jobs (
            job_id CHAR(36) NOT NULL PRIMARY KEY,
            queue VARCHAR(128) NOT NULL,
            contract_name VARCHAR(256) NOT NULL,
            contract_version INT NOT NULL,
            payload TEXT NOT NULL,
            state TINYINT NOT NULL,
            eligible_at DATETIME(6) NULL,
            attempt_count INT NOT NULL,
            max_attempts INT NOT NULL,
            cancellation_requested TINYINT(1) NOT NULL,
            created_at DATETIME(6) NOT NULL,
            completed_at DATETIME(6) NULL,
            last_error TEXT NULL,
            idempotency_key VARCHAR(256) NULL,
            idempotency_expires_at DATETIME(6) NULL,
            lease_token CHAR(36) NULL,
            INDEX ix_djk_dispatch (queue, eligible_at, job_id),
            UNIQUE INDEX ux_djk_active_idempotency (idempotency_key, state)
        );
        """;
}
