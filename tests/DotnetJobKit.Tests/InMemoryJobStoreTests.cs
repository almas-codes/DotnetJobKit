using DotnetJobKit.Abstractions;
using DotnetJobKit.Storage;

namespace DotnetJobKit.Tests;

public class InMemoryJobStoreTests
{
    [Fact]
    public async Task Expired_lease_with_exhausted_attempts_becomes_dead()
    {
        var store = new InMemoryJobStore();
        var now = DateTimeOffset.UtcNow;
        var jobId = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "test.job",
            ContractVersion = 1,
            Payload = "{}",
            MaxAttempts = 1,
        }, now, CancellationToken.None);

        var claimed = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None);
        Assert.Single(claimed);
        Assert.Equal(1, claimed[0].AttemptCount);

        var expired = now.AddHours(1);
        var recovery = await store.ClaimAsync(["default"], 1, expired, TimeSpan.FromMinutes(1), CancellationToken.None);
        Assert.Empty(recovery);

        var record = await store.GetAsync(jobId, CancellationToken.None);
        Assert.NotNull(record);
        Assert.Equal(JobState.Dead, record!.State);
    }

    [Fact]
    public async Task Stale_owner_cannot_settle_newer_attempt()
    {
        var store = new InMemoryJobStore();
        var now = DateTimeOffset.UtcNow;
        await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "test.job",
            ContractVersion = 1,
            Payload = "{}",
            MaxAttempts = 3,
        }, now, CancellationToken.None);

        var first = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None);
        var attempt1 = first[0].AttemptCount;

        var expired = now.AddHours(1);
        var second = await store.ClaimAsync(["default"], 1, expired, TimeSpan.FromMinutes(1), CancellationToken.None);
        Assert.Single(second);
        Assert.True(second[0].AttemptCount > attempt1);

        var staleSettle = await store.SettleAsync(new SettleRequest
        {
            JobId = first[0].JobId,
            AttemptCount = attempt1,
            Outcome = SettleOutcome.Succeeded,
            Now = expired,
            DeleteOnSuccess = false,
        }, CancellationToken.None);

        Assert.False(staleSettle);
    }
}
