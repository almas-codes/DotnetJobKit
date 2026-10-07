using DotnetJobKit.Execution;

namespace DotnetJobKit.Tests;

public class DeadlineEngineTests
{
    [Fact]
    public void DrainDue_processes_earliest_first()
    {
        var engine = new DeadlineEngine();
        var jobA = Guid.NewGuid();
        var jobB = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        engine.Schedule(new DeadlineEvent(jobA, 1, DeadlineKind.LeaseMaintenance, 1, now.AddSeconds(5)));
        engine.Schedule(new DeadlineEvent(jobB, 1, DeadlineKind.LeaseMaintenance, 1, now.AddSeconds(1)));

        var due = engine.DrainDue(now.AddSeconds(2));
        Assert.Single(due);
        Assert.Equal(jobB, due[0].JobId);
    }

    [Fact]
    public void Stale_generation_is_ignored()
    {
        var engine = new DeadlineEngine();
        var jobId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        var gen = engine.BumpGeneration(jobId);
        engine.Schedule(new DeadlineEvent(jobId, 1, DeadlineKind.LeaseMaintenance, gen, now));
        engine.BumpGeneration(jobId);

        var due = engine.DrainDue(now.AddMinutes(1));
        Assert.Empty(due);
    }

    [Fact]
    public void Invalidate_prevents_completed_job_maintenance()
    {
        var engine = new DeadlineEngine();
        var jobId = Guid.NewGuid();
        var gen = engine.BumpGeneration(jobId);
        var now = DateTimeOffset.UtcNow;
        engine.Schedule(new DeadlineEvent(jobId, 1, DeadlineKind.LeaseMaintenance, gen, now));
        engine.Invalidate(jobId);

        Assert.Empty(engine.DrainDue(now.AddMinutes(1)));
    }

    [Fact]
    public void PeekNextDueUtc_returns_earliest_valid()
    {
        var engine = new DeadlineEngine();
        var now = DateTimeOffset.UtcNow;
        engine.Schedule(new DeadlineEvent(Guid.NewGuid(), 1, DeadlineKind.LeaseMaintenance, 1, now.AddSeconds(30)));
        engine.Schedule(new DeadlineEvent(Guid.NewGuid(), 1, DeadlineKind.LeaseMaintenance, 1, now.AddSeconds(10)));

        Assert.Equal(now.AddSeconds(10), engine.PeekNextDueUtc());
    }
}
