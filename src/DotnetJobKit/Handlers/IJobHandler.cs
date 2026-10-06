namespace DotnetJobKit.Handlers;

public interface IJobHandler<in TJob>
    where TJob : notnull
{
    ValueTask HandleAsync(TJob job, JobContext context, CancellationToken cancellationToken);
}
