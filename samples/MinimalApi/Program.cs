using System.Text;
using DotnetJobKit.Abstractions;
using DotnetJobKit.Client;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.Sqlite;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDotnetJobKit(options =>
{
    options.MaxConcurrency = 4;
    options.Queues = ["default"];
    options.Retention.DeleteOnSuccess = false;
    options.Retention.SucceededRetention = TimeSpan.FromDays(7);
});

builder.Services.AddDotnetJobKitSqlite(builder.Configuration.GetConnectionString("JobKit") ?? "Data Source=jobkit.db");
builder.Services.AddDotnetJobHandler<ApiPingJob, ApiPingJobHandler>("api.ping", "default");

var app = builder.Build();

app.MapPost("/jobs/ping", async (IJobSubmitter jobs, ApiPingJob body) =>
{
    var id = await jobs.EnqueueAsync(body);
    return Results.Accepted($"/jobs/{id}", new { id });
});

app.MapGet("/jobs/{id:guid}", async (Guid id, IJobStore store) =>
{
    var job = await store.GetAsync(id, CancellationToken.None);
    return job is null ? Results.NotFound() : Results.Ok(job);
});

app.MapGet("/admin/api/summary", async (IJobStore store) =>
{
    var counts = await store.GetCountsByStateAsync(CancellationToken.None);
    return Results.Ok(counts.ToDictionary(k => k.Key.ToString(), v => v.Value));
});

app.MapGet("/admin/api/jobs", async (IJobStore store, JobState? state, string? queue, int? limit) =>
{
    var jobs = await store.ListJobsAsync(state, queue, limit ?? 100, CancellationToken.None);
    return Results.Ok(jobs);
});

app.MapPost("/admin/api/jobs/{id:guid}/retry", async (Guid id, IJobStore store, IJobWakeSignal wake) =>
{
    var ok = await store.RequeueDeadAsync(id, DateTimeOffset.UtcNow, CancellationToken.None);
    if (ok)
        wake.Notify();
    return ok ? Results.Ok() : Results.NotFound();
});

app.MapDelete("/admin/api/jobs/{id:guid}", async (Guid id, IJobStore store) =>
{
    var ok = await store.DeleteTerminalJobAsync(id, CancellationToken.None);
    return ok ? Results.NoContent() : Results.NotFound();
});

app.MapGet("/admin", () => Results.Content(DashboardHtml, "text/html", Encoding.UTF8));

app.Run();

const string DashboardHtml = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>DotnetJobKit Dashboard</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 1.5rem; }
    table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
    th, td { border: 1px solid #ccc; padding: 0.4rem 0.6rem; text-align: left; }
    .counts span { margin-right: 1rem; }
  </style>
</head>
<body>
  <h1>DotnetJobKit</h1>
  <div class="counts" id="counts">Loading…</div>
  <label>State <select id="state"><option value="">All</option><option>Ready</option><option>Leased</option><option>Succeeded</option><option>Dead</option><option>Cancelled</option></select></label>
  <button onclick="load()">Refresh</button>
  <table><thead><tr><th>Id</th><th>Queue</th><th>Contract</th><th>State</th><th>Attempts</th><th>Eligible</th><th>Actions</th></tr></thead><tbody id="rows"></tbody></table>
  <script>
    async function load() {
      const counts = await fetch('/admin/api/summary').then(r => r.json());
      document.getElementById('counts').innerHTML = Object.entries(counts).map(([k,v]) => `<span><b>${k}</b>: ${v}</span>`).join('');
      const state = document.getElementById('state').value;
      const q = state ? `?state=${state}` : '';
      const jobs = await fetch('/admin/api/jobs' + q).then(r => r.json());
      document.getElementById('rows').innerHTML = jobs.map(j => `<tr>
        <td>${j.jobId}</td><td>${j.queue}</td><td>${j.contractName}</td><td>${j.state}</td>
        <td>${j.attemptCount}/${j.maxAttempts}</td><td>${j.eligibleAt ?? ''}</td>
        <td>${j.state === 'Dead' ? `<button onclick="retry('${j.jobId}')">Retry</button> <button onclick="del('${j.jobId}')">Delete</button>` : ''}</td>
      </tr>`).join('');
    }
    async function retry(id) { await fetch('/admin/api/jobs/' + id + '/retry', { method: 'POST' }); load(); }
    async function del(id) { await fetch('/admin/api/jobs/' + id, { method: 'DELETE' }); load(); }
    load();
  </script>
</body>
</html>
""";

public sealed record ApiPingJob(string Message);

public sealed class ApiPingJobHandler(ILogger<ApiPingJobHandler> logger) : IJobHandler<ApiPingJob>
{
    public Task HandleAsync(ApiPingJob job, JobContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation("API ping job {JobId}: {Message}", context.JobId, job.Message);
        return Task.CompletedTask;
    }
}
