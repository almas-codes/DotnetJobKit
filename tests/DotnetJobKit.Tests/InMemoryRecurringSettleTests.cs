using DotnetJobKit.Abstractions;
using DotnetJobKit.Storage;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.Tests;

public class InMemoryRecurringSettleTests
{
    [Fact]
    public async Task CompleteAsRecurringReady_after_claim_succeeds()
    {
        var store = new InMemoryJobStore(Options.Create(new InMemoryJobStoreOptions()));
        var now = new DateTimeOffset(2026, 5, 1, 10, 0, 0, TimeSpan.Zero);
        var jobId = await store.SubmitAsync(
            new JobSubmitRequest
            {
                Queue = "default",
                ContractName = "test.recurring",
                ContractVersion = 1,
                Payload = "{}",
                RecurrenceCron = "* * * * *",
            },
            now,
            CancellationToken.None);

        var claimed = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(2), CancellationToken.None);
        Assert.Single(claimed);
        var c = claimed[0];
        Assert.Equal("* * * * *", c.RecurrenceCron);

        var next = now.AddMinutes(1);
        var ok = await store.CompleteAsRecurringReadyAsync(
            c.JobId,
            c.AttemptCount,
            c.LeaseToken,
            next,
            now,
            CancellationToken.None);
        Assert.True(ok);

        var record = await store.GetAsync(jobId, CancellationToken.None);
        Assert.NotNull(record);
        Assert.Equal(JobState.Ready, record!.State);
        Assert.Equal(0, record.AttemptCount);
        Assert.Equal(next, record.EligibleAt);
    }
}
