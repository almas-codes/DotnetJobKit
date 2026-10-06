using System.Data.Common;
using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Handlers;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.Client;

public sealed class JobSubmitter : IJobSubmitter
{
    private readonly IJobStore _store;
    private readonly JobHandlerRegistry _registry;
    private readonly IJobWakeSignal _wakeSignal;
    private readonly TimeProvider _timeProvider;
    private readonly DotnetJobKitOptions _options;

    public JobSubmitter(
        IJobStore store,
        JobHandlerRegistry registry,
        IJobWakeSignal wakeSignal,
        TimeProvider timeProvider,
        IOptions<DotnetJobKitOptions> options)
    {
        _store = store;
        _registry = registry;
        _wakeSignal = wakeSignal;
        _timeProvider = timeProvider;
        _options = options.Value;
    }

    public async Task<Guid> EnqueueAsync<TJob>(TJob job, CancellationToken cancellationToken = default)
        where TJob : notnull
    {
        var now = _timeProvider.GetUtcNow();
        return await SubmitCoreAsync(job, now, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Guid> ScheduleAsync<TJob>(TJob job, DateTimeOffset eligibleAt, CancellationToken cancellationToken = default)
        where TJob : notnull
    {
        var now = _timeProvider.GetUtcNow();
        return await SubmitCoreAsync(job, eligibleAt, cancellationToken).ConfigureAwait(false);
    }

    private async Task<Guid> SubmitCoreAsync<TJob>(TJob job, DateTimeOffset eligibleAt, CancellationToken cancellationToken)
        where TJob : notnull
    {
        var registration = _registry.GetByJobType(typeof(TJob));
        var payload = _registry.Serialize(job);
        ValidatePayloadSize(payload);

        var request = new JobSubmitRequest
        {
            Queue = registration.DefaultQueue,
            ContractName = registration.ContractName,
            ContractVersion = registration.ContractVersion,
            Payload = payload,
            EligibleAt = eligibleAt,
            MaxAttempts = _options.DefaultRetry.MaxAttempts,
        };

        var jobId = await _store.SubmitAsync(request, _timeProvider.GetUtcNow(), cancellationToken)
            .ConfigureAwait(false);
        _wakeSignal.Notify();
        return jobId;
    }

    public async Task<Guid> EnqueueInTransactionAsync<TJob>(
        TJob job,
        DbConnection connection,
        DbTransaction? transaction,
        CancellationToken cancellationToken = default)
        where TJob : notnull
    {
        if (_store is not IEnlistedJobStore enlisted)
        {
            throw new InvalidOperationException(
                "The configured job store does not support transactional enqueue. Use a SQLite enlisted store.");
        }

        var registration = _registry.GetByJobType(typeof(TJob));
        var payload = _registry.Serialize(job);
        ValidatePayloadSize(payload);

        var request = new JobSubmitRequest
        {
            Queue = registration.DefaultQueue,
            ContractName = registration.ContractName,
            ContractVersion = registration.ContractVersion,
            Payload = payload,
            EligibleAt = _timeProvider.GetUtcNow(),
            MaxAttempts = _options.DefaultRetry.MaxAttempts,
        };

        return await enlisted.SubmitAsync(
            request,
            _timeProvider.GetUtcNow(),
            connection,
            transaction,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> CancelAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        var cancelled = await _store.CancelReadyAsync(jobId, now, cancellationToken).ConfigureAwait(false);
        if (!cancelled)
            cancelled = await _store.RequestLeasedCancellationAsync(jobId, cancellationToken).ConfigureAwait(false);

        if (cancelled)
            _wakeSignal.Notify();

        return cancelled;
    }

    private void ValidatePayloadSize(string payload)
    {
        var byteCount = System.Text.Encoding.UTF8.GetByteCount(payload);
        if (byteCount > _options.PayloadMaxBytes)
            throw new InvalidOperationException(
                $"Job payload size {byteCount} bytes exceeds limit {_options.PayloadMaxBytes}.");
    }
}
