using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Storage;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.IntegrationTests;

public class FailureScenarioTests
{
    private static InMemoryJobStore CreateStore() =>
        new(Options.Create(new InMemoryJobStoreOptions()));

    [Fact]
    public async Task Crash_after_claim_leaves_leased_row_recoverable()
    {
        var store = CreateStore();
        var now = DateTimeOffset.UtcNow;
        await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "t",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        var claimed = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.Single(claimed);

        var expired = now.AddMinutes(1);
        var recovered = await store.ClaimAsync(["default"], 1, expired, TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.Single(recovered);
        Assert.Equal(2, recovered[0].AttemptCount);
    }

    [Fact]
    public async Task Retry_exhaustion_on_expired_lease_becomes_dead()
    {
        var store = CreateStore();
        var now = DateTimeOffset.UtcNow;
        var id = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "t",
            ContractVersion = 1,
            Payload = "{}",
            MaxAttempts = 2,
        }, now, CancellationToken.None);

        await store.ClaimAsync(["default"], 1, now, TimeSpan.FromSeconds(1), CancellationToken.None);
        await store.ClaimAsync(["default"], 1, now.AddMinutes(1), TimeSpan.FromSeconds(1), CancellationToken.None);
        _ = await store.ClaimAsync(["default"], 1, now.AddMinutes(2), TimeSpan.FromSeconds(1), CancellationToken.None);

        var record = await store.GetAsync(id, CancellationToken.None);
        Assert.Equal(JobState.Dead, record!.State);
    }

    [Fact]
    public async Task Ready_cancel_is_immediate()
    {
        var store = CreateStore();
        var now = DateTimeOffset.UtcNow;
        var id = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "t",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        Assert.True(await store.CancelReadyAsync(id, now, CancellationToken.None));
        var record = await store.GetAsync(id, CancellationToken.None);
        Assert.Equal(JobState.Cancelled, record!.State);
    }

    [Fact]
    public async Task Leased_cancel_sets_flag_observable_by_poll()
    {
        var store = CreateStore();
        var now = DateTimeOffset.UtcNow;
        var id = await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "t",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        var claimed = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromMinutes(1), CancellationToken.None);
        Assert.True(await store.RequestLeasedCancellationAsync(id, CancellationToken.None));
        Assert.True(await store.IsCancellationRequestedAsync(
            id,
            claimed[0].AttemptCount,
            claimed[0].LeaseToken,
            CancellationToken.None));
    }

    [Fact]
    public async Task Idempotency_key_returns_same_job()
    {
        var store = CreateStore();
        var now = DateTimeOffset.UtcNow;
        var request = new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "t",
            ContractVersion = 1,
            Payload = "{}",
            IdempotencyKey = "key-1",
        };

        var a = await store.SubmitAsync(request, now, CancellationToken.None);
        var b = await store.SubmitAsync(request, now, CancellationToken.None);
        Assert.Equal(a, b);
    }

    [Fact]
    public async Task Renew_fails_for_stale_attempt()
    {
        var store = CreateStore();
        var now = DateTimeOffset.UtcNow;
        await store.SubmitAsync(new JobSubmitRequest
        {
            Queue = "default",
            ContractName = "t",
            ContractVersion = 1,
            Payload = "{}",
        }, now, CancellationToken.None);

        var first = await store.ClaimAsync(["default"], 1, now, TimeSpan.FromSeconds(1), CancellationToken.None);
        await store.ClaimAsync(["default"], 1, now.AddMinutes(5), TimeSpan.FromSeconds(1), CancellationToken.None);

        var renewed = await store.RenewAsync(
            first[0].JobId,
            first[0].AttemptCount,
            first[0].LeaseToken,
            now.AddMinutes(2),
            CancellationToken.None);
        Assert.False(renewed);
    }
}
