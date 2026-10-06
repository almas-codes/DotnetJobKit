namespace DotnetJobKit.Abstractions;

public interface IJobSubmitter
{
    Task<Guid> EnqueueAsync<TJob>(TJob job, CancellationToken cancellationToken = default)
        where TJob : notnull;

    Task<Guid> ScheduleAsync<TJob>(TJob job, DateTimeOffset eligibleAt, CancellationToken cancellationToken = default)
        where TJob : notnull;

    Task<bool> CancelAsync(Guid jobId, CancellationToken cancellationToken = default);
}
