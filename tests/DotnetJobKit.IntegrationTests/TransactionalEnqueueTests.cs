using DotnetJobKit.Abstractions;
using DotnetJobKit.Client;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.EntityFrameworkCore;
using DotnetJobKit.Handlers;
using DotnetJobKit.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.IntegrationTests;

public sealed record TxJob(string Value);

public sealed class TxJobHandler : IJobHandler<TxJob>
{
    public Task HandleAsync(TxJob job, JobContext context, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}

public sealed class TxDbContext(DbContextOptions<TxDbContext> options) : DbContext(options)
{
    public DbSet<TxLedgerRow> Ledger { get; set; } = null!;
}

public sealed class TxLedgerRow
{
    public int Id { get; set; }
    public string Value { get; set; } = string.Empty;
}

public class TransactionalEnqueueTests
{
    [Fact]
    public async Task Ef_transaction_rollbacks_job_and_ledger_together()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"djk-tx-{Guid.NewGuid():N}.db");

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TxDbContext>(o => o.UseSqlite($"Data Source={dbPath}"));
        builder.Services.AddDotnetJobKit(o => o.MaxConcurrency = 1);
        builder.Services.AddDotnetJobKitSqlite($"Data Source={dbPath}");
        builder.Services.AddDotnetJobHandler<TxJob, TxJobHandler>("tests.tx", "default");

        using var host = builder.Build();
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
            await db.Database.EnsureCreatedAsync();
            var sqlite = db.Database.GetDbConnection() as SqliteConnection
                ?? throw new InvalidOperationException("Expected SQLite provider.");
            if (sqlite.State != System.Data.ConnectionState.Open)
                await sqlite.OpenAsync();
            SqliteJobStore.EnsureSchema(sqlite);
        }

        await host.StartAsync();
        var submitter = host.Services.GetRequiredService<IJobSubmitter>();

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
            await using var tx = await db.Database.BeginTransactionAsync();
            db.Ledger.Add(new TxLedgerRow { Value = "rolled-back" });
            _ = await submitter.EnqueueInTransactionAsync(new TxJob("x"), db, CancellationToken.None);
            await tx.RollbackAsync();
        }

        await using (var connection = new SqliteConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM Jobs;";
            var jobCount = (long)(await cmd.ExecuteScalarAsync() ?? 0L);
            Assert.Equal(0, jobCount);
        }

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
            Assert.Equal(0, await db.Ledger.CountAsync());
        }

        await host.StopAsync();
    }

    [Fact]
    public async Task Ef_transaction_commit_persists_job()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"djk-tx2-{Guid.NewGuid():N}.db");

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TxDbContext>(o => o.UseSqlite($"Data Source={dbPath}"));
        builder.Services.AddDotnetJobKit(o => o.MaxConcurrency = 1);
        builder.Services.AddDotnetJobKitSqlite($"Data Source={dbPath}");
        builder.Services.AddDotnetJobHandler<TxJob, TxJobHandler>("tests.tx", "default");

        using var host = builder.Build();
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
            await db.Database.EnsureCreatedAsync();
            var sqlite = db.Database.GetDbConnection() as SqliteConnection
                ?? throw new InvalidOperationException("Expected SQLite provider.");
            if (sqlite.State != System.Data.ConnectionState.Open)
                await sqlite.OpenAsync();
            SqliteJobStore.EnsureSchema(sqlite);
        }

        await host.StartAsync();
        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        var wake = host.Services.GetRequiredService<IJobWakeSignal>();

        Guid jobId;
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
            await using var tx = await db.Database.BeginTransactionAsync();
            db.Ledger.Add(new TxLedgerRow { Value = "committed" });
            jobId = await submitter.EnqueueInTransactionAsync(new TxJob("x"), db, CancellationToken.None);
            await db.SaveChangesAsync();
            await tx.CommitAsync();
        }

        wake.Notify();

        await using (var connection = new SqliteConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM Jobs;";
            var jobCount = (long)(await cmd.ExecuteScalarAsync() ?? 0L);
            Assert.Equal(1, jobCount);
        }

        await host.StopAsync();
    }

    [Fact]
    public async Task Ef_enqueue_uses_dbcontext_connection_instance()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"djk-tx-conn-{Guid.NewGuid():N}.db");

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TxDbContext>(o => o.UseSqlite($"Data Source={dbPath}"));
        builder.Services.AddDotnetJobKit();
        builder.Services.AddDotnetJobKitSqlite($"Data Source={dbPath}");
        builder.Services.AddDotnetJobHandler<TxJob, TxJobHandler>("tests.tx", "default");

        using var host = builder.Build();
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
            await db.Database.EnsureCreatedAsync();
            var sqlite = db.Database.GetDbConnection() as SqliteConnection ?? throw new InvalidOperationException();
            if (sqlite.State != System.Data.ConnectionState.Open)
                await sqlite.OpenAsync();
            SqliteJobStore.EnsureSchema(sqlite);
        }

        var submitter = host.Services.GetRequiredService<IJobSubmitter>();
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
            var connectionBefore = db.Database.GetDbConnection();
            await using var tx = await db.Database.BeginTransactionAsync();
            _ = await submitter.EnqueueInTransactionAsync(new TxJob("x"), db, CancellationToken.None);
            Assert.Same(connectionBefore, db.Database.GetDbConnection());
            await tx.RollbackAsync();
        }
    }
}
