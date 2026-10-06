using System.Diagnostics;
using DotnetJobKit.Sqlite;
using Microsoft.Data.Sqlite;

namespace DotnetJobKit.IntegrationTests;

public class SqliteMultiProcessTests
{
    [Fact]
    public async Task Multiple_processes_claim_distinct_jobs_from_shared_db()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"djk-mp-{Guid.NewGuid():N}.db");
        var cs = $"Data Source={dbPath}";

        await using (var connection = new SqliteConnection(cs))
        {
            await connection.OpenAsync();
            SqliteJobStore.EnsureSchema(connection);
            await SqliteJobBulkInserter.InsertReadyJobsAsync(
                connection, 40, "default", "perf.job", 1, "{}", DateTimeOffset.UtcNow, 1, batchSize: 20);
        }

        var workerProject = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory,
            "..", "..", "..", "..", "..",
            "tools", "DotnetJobKit.SqliteMultiProcessWorker", "DotnetJobKit.SqliteMultiProcessWorker.csproj"));

        if (!File.Exists(workerProject))
            return;

        var processes = new List<Process>();
        for (var i = 0; i < 4; i++)
        {
            var psi = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = $"run --project \"{workerProject}\" -c Release -- \"{dbPath}\" 20",
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            processes.Add(Process.Start(psi)!);
        }

        var totalClaimed = 0;
        foreach (var p in processes)
        {
            var output = await p.StandardOutput.ReadToEndAsync();
            await p.WaitForExitAsync();
            var line = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).LastOrDefault() ?? "";
            var claimedPart = line.Split(';').FirstOrDefault(s => s.StartsWith("CLAIMED=", StringComparison.Ordinal));
            if (claimedPart is not null)
                totalClaimed += int.Parse(claimedPart["CLAIMED=".Length..]);
        }

        Assert.InRange(totalClaimed, 20, 40);
    }
}
