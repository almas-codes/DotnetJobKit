namespace DotnetJobKit.Handlers;

public interface IJobHandler<in TJob>
    where TJob : notnull
{
    Task HandleAsync(TJob job, JobContext context, CancellationToken cancellationToken);
}
