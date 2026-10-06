using DotnetJobKit.Abstractions;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.Sqlite;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDotnetJobKit(options =>
{
    options.MaxConcurrency = 4;
    options.Queues = ["default"];
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

app.Run();

public sealed record ApiPingJob(string Message);

public sealed class ApiPingJobHandler(ILogger<ApiPingJobHandler> logger) : IJobHandler<ApiPingJob>
{
    public ValueTask HandleAsync(ApiPingJob job, JobContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation("API ping job {JobId}: {Message}", context.JobId, job.Message);
        return ValueTask.CompletedTask;
    }
}
