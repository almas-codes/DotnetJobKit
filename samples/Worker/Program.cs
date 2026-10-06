using DotnetJobKit.Abstractions;
using DotnetJobKit.Client;
using DotnetJobKit.Configuration;
using DotnetJobKit.DependencyInjection;
using DotnetJobKit.Handlers;
using DotnetJobKit.Sqlite;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDotnetJobKit(options =>
{
    options.MaxConcurrency = 8;
    options.Queues = ["default", "notifications"];
    options.Retention.DeleteOnSuccess = true;
});

var dbPath = Path.Combine(AppContext.BaseDirectory, "jobkit.db");
builder.Services.AddDotnetJobKitSqlite($"Data Source={dbPath}");

builder.Services.AddDotnetJobHandler<SampleJob, SampleJobHandler>(
    contract: "samples.hello",
    queue: "default");

builder.Services.AddDotnetJobHandler<ScheduledSampleJob, ScheduledSampleJobHandler>(
    contract: "samples.scheduled",
    queue: "notifications");

var host = builder.Build();

if (args.Contains("--enqueue-demo") || args.Contains("--demo-all"))
{
    var submitter = host.Services.GetRequiredService<IJobSubmitter>();
    await submitter.EnqueueAsync(new SampleJob("DotnetJobKit worker sample"));
}

if (args.Contains("--demo-all"))
{
    var submitter = host.Services.GetRequiredService<IJobSubmitter>();
    await submitter.ScheduleAsync(
        new ScheduledSampleJob("ConnectBE-style delayed reminder"),
        DateTimeOffset.UtcNow.AddSeconds(15));
    await submitter.EnqueueRecurringAsync(new SampleJob("recurring heartbeat"), "*/5 * * * *");
    await submitter.EnqueueWithUniqueKeyAsync(new SampleJob("idempotent-once"), "demo-unique-key");
}

host.Run();

public sealed record ScheduledSampleJob(string Message);

public sealed class ScheduledSampleJobHandler(ILogger<ScheduledSampleJobHandler> logger) : IJobHandler<ScheduledSampleJob>
{
    public Task HandleAsync(ScheduledSampleJob job, JobContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation("Scheduled sample fired: {Message}", job.Message);
        return Task.CompletedTask;
    }
}

public sealed record SampleJob(string Message);

public sealed class SampleJobHandler(ILogger<SampleJobHandler> logger) : IJobHandler<SampleJob>
{
    public Task HandleAsync(SampleJob job, JobContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Handled job {JobId} attempt {Attempt}: {Message}",
            context.JobId,
            context.AttemptCount,
            job.Message);
        return Task.CompletedTask;
    }
}
