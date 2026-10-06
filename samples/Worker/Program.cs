using DotnetJobKit.Abstractions;
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

var host = builder.Build();

if (args.Contains("--enqueue-demo"))
{
    var submitter = host.Services.GetRequiredService<IJobSubmitter>();
    await submitter.EnqueueAsync(new SampleJob("DotnetJobKit worker sample"));
}

host.Run();

public sealed record SampleJob(string Message);

public sealed class SampleJobHandler(ILogger<SampleJobHandler> logger) : IJobHandler<SampleJob>
{
    public ValueTask HandleAsync(SampleJob job, JobContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Handled job {JobId} attempt {Attempt}: {Message}",
            context.JobId,
            context.AttemptCount,
            job.Message);
        return ValueTask.CompletedTask;
    }
}
