using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Sqlite;
using Microsoft.Extensions.Options;

if (args.Length < 2)
{
    Console.WriteLine("Usage: SqliteMultiProcessWorker <dbPath> <maxClaims>");
    return 1;
}

var dbPath = args[0];
var maxClaims = int.Parse(args[1]);
var cs = $"Data Source={dbPath}";

await using var connection = new Microsoft.Data.Sqlite.SqliteConnection(cs);
await connection.OpenAsync();
SqliteJobStore.EnsureSchema(connection);

using var store = new SqliteJobStore(Options.Create(new SqliteJobStoreOptions { ConnectionString = cs }));
var claimed = 0;
var sw = System.Diagnostics.Stopwatch.StartNew();
while (claimed < maxClaims)
{
    var batch = await store.ClaimAsync(["default"], 1, DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1), CancellationToken.None);
    if (batch.Count == 0)
        break;
    claimed += batch.Count;
}

sw.Stop();
Console.WriteLine($"CLAIMED={claimed};MS={sw.ElapsedMilliseconds}");
return 0;
