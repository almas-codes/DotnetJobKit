using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Handlers;
using DotnetJobKit.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.Runtime;

public sealed class JobRuntimeCoordinator : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IJobStore _store;
    private readonly JobHandlerRegistry _registry;
    private readonly IJobWakeSignal _wakeSignal;
    private readonly TimeProvider _timeProvider;
    private readonly DotnetJobKitOptions _options;
    private readonly ILogger<JobRuntimeCoordinator> _logger;
    private readonly ActiveLeaseTracker _leases = new();
    private int _activeExecutions;

    public JobRuntimeCoordinator(
        IServiceProvider serviceProvider,
        IJobStore store,
        JobHandlerRegistry registry,
        IJobWakeSignal wakeSignal,
        TimeProvider timeProvider,
        IOptions<DotnetJobKitOptions> options,
        ILogger<JobRuntimeCoordinator> logger)
    {
        _serviceProvider = serviceProvider;
        _store = store;
        _registry = registry;
        _wakeSignal = wakeSignal;
        _timeProvider = timeProvider;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DotnetJobKit runtime started (max concurrency {MaxConcurrency})", _options.MaxConcurrency);

        var nextReconcileAt = _timeProvider.GetUtcNow() + _options.ReconciliationInterval;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var now = _timeProvider.GetUtcNow();
                await ProcessRenewalsAsync(now, stoppingToken).ConfigureAwait(false);

                while (Volatile.Read(ref _activeExecutions) < _options.MaxConcurrency && !stoppingToken.IsCancellationRequested)
                {
                    var capacity = _options.MaxConcurrency - Volatile.Read(ref _activeExecutions);
                    var claimed = await _store.ClaimAsync(
                            _options.Queues,
                            capacity,
                            now,
                            _options.LeaseDuration,
                            stoppingToken)
                        .ConfigureAwait(false);

                    if (claimed.Count == 0)
                        break;

                    foreach (var job in claimed)
                    {
                        Interlocked.Increment(ref _activeExecutions);
                        _ = ExecuteClaimedJobAsync(job, stoppingToken);
                    }

                    now = _timeProvider.GetUtcNow();
                }

                if (now >= nextReconcileAt)
                {
                    var retention = new JobRetentionPurge
                    {
                        SucceededRetention = _options.Retention.SucceededRetention,
                        FailedRetention = _options.Retention.FailedRetention,
                        CancelledRetention = _options.Retention.CancelledRetention,
                    };
                    await _store.DeleteTerminalBatchAsync(
                            now,
                            retention,
                            _options.Retention.PurgeBatchSize,
                            stoppingToken)
                        .ConfigureAwait(false);
                    nextReconcileAt = now + _options.ReconciliationInterval;
                }

                if (Volatile.Read(ref _activeExecutions) > 0)
                {
                    await _wakeSignal.WaitUntilAsync(now.AddMilliseconds(250), stoppingToken).ConfigureAwait(false);
                    continue;
                }

                var nextEligible = await _store.GetNextEligibleAtAsync(_options.Queues, now, stoppingToken)
                    .ConfigureAwait(false);
                var nextRenew = _leases.GetNextRenewAt();
                var wakeCandidates = new List<DateTimeOffset> { nextReconcileAt, nextRenew };
                if (nextEligible is not null)
                    wakeCandidates.Add(nextEligible.Value);

                var wakeAt = wakeCandidates.Min();
                wakeAt = CapWakeTime(wakeAt, now);

                if (nextEligible is null && wakeAt >= now.AddYears(10))
                {
                    await _wakeSignal.WaitForSignalAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await _wakeSignal.WaitUntilAsync(wakeAt, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DotnetJobKit coordinator loop failed");
                var backoff = TimeSpan.FromSeconds(_options.ErrorBackoffSeconds > 0 ? _options.ErrorBackoffSeconds : 30);
                try
                {
                    await Task.Delay(backoff, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        await WaitForActiveExecutionsAsync(stoppingToken).ConfigureAwait(false);
        _logger.LogInformation("DotnetJobKit runtime stopped");
    }

    private async Task ExecuteClaimedJobAsync(ClaimedJob claimed, CancellationToken stoppingToken)
    {
        var renewAt = _timeProvider.GetUtcNow() + _options.LeaseRenewInterval;
        _leases.Add(claimed.JobId, claimed.AttemptCount, claimed.LeaseToken, renewAt, claimed.LeaseExpiresAt);

        using var executionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        if (_options.ExecutionTimeout > TimeSpan.Zero)
            executionCts.CancelAfter(_options.ExecutionTimeout);

        try
        {
            if (claimed.CancellationRequested)
                executionCts.Cancel();

            using var scope = _serviceProvider.CreateScope();
            var jobObject = _registry.Deserialize(claimed.ContractName, claimed.ContractVersion, claimed.Payload);
            var registration = _registry.GetByContract(claimed.ContractName, claimed.ContractVersion);
            var handler = scope.ServiceProvider.GetRequiredService(registration.HandlerType);

            var context = new JobContext
            {
                JobId = claimed.JobId,
                Queue = claimed.Queue,
                ContractName = claimed.ContractName,
                AttemptCount = claimed.AttemptCount,
                MaxAttempts = claimed.MaxAttempts,
            };

            await HandlerExecutionRunner.RunAsync(
                _store,
                claimed,
                handler,
                registration.JobType,
                jobObject,
                context,
                _options.CancellationPollInterval,
                _options.LeaseDuration,
                _options.LeaseRenewInterval,
                _timeProvider,
                executionCts.Token,
                stoppingToken).ConfigureAwait(false);

            await SettleSuccessAsync(claimed, CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (executionCts.IsCancellationRequested && !stoppingToken.IsCancellationRequested)
        {
            await SettleCancelledAsync(claimed, "Execution cancelled or timed out.", CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Job {JobId} ({Contract}) failed on attempt {Attempt}", claimed.JobId, claimed.ContractName, claimed.AttemptCount);
            await SettleFailureAsync(claimed, ex.Message, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _leases.Remove(claimed.JobId);
            Interlocked.Decrement(ref _activeExecutions);
            _wakeSignal.Notify();
        }
    }

    private async Task SettleSuccessAsync(ClaimedJob claimed, CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();

        if (claimed.RecurrenceCron is { Length: > 0 } cron)
        {
            var next = CronFiveFieldParser.GetNextOccurrence(cron, now);
            await _store.CompleteAsRecurringReadyAsync(
                    claimed.JobId,
                    claimed.AttemptCount,
                    claimed.LeaseToken,
                    next,
                    now,
                    cancellationToken)
                .ConfigureAwait(false);
            _wakeSignal.Notify();
            await EnqueueContinuationIfPresentAsync(claimed, cancellationToken).ConfigureAwait(false);
            return;
        }

        await _store.SettleAsync(new SettleRequest
        {
            JobId = claimed.JobId,
            AttemptCount = claimed.AttemptCount,
            LeaseToken = claimed.LeaseToken,
            Outcome = SettleOutcome.Succeeded,
            Now = now,
            DeleteOnSuccess = _options.Retention.ShouldDeleteImmediatelyOnSuccess(),
        }, cancellationToken).ConfigureAwait(false);

        await EnqueueContinuationIfPresentAsync(claimed, cancellationToken).ConfigureAwait(false);
    }

    private async Task EnqueueContinuationIfPresentAsync(ClaimedJob claimed, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(claimed.ContinuationContractName)
            || string.IsNullOrWhiteSpace(claimed.ContinuationPayload))
        {
            return;
        }

        var request = new JobSubmitRequest
        {
            Queue = claimed.ContinuationQueue ?? claimed.Queue,
            ContractName = claimed.ContinuationContractName,
            ContractVersion = claimed.ContinuationContractVersion ?? 1,
            Payload = claimed.ContinuationPayload,
            MaxAttempts = _options.DefaultRetry.MaxAttempts,
        };

        await _store.SubmitAsync(request, _timeProvider.GetUtcNow(), cancellationToken).ConfigureAwait(false);
        _wakeSignal.Notify();
    }

    private async Task SettleCancelledAsync(ClaimedJob claimed, string reason, CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        await _store.SettleAsync(new SettleRequest
        {
            JobId = claimed.JobId,
            AttemptCount = claimed.AttemptCount,
            LeaseToken = claimed.LeaseToken,
            Outcome = SettleOutcome.Cancelled,
            LastError = reason,
            Now = now,
            DeleteOnSuccess = false,
        }, cancellationToken).ConfigureAwait(false);
    }

    private async Task SettleFailureAsync(ClaimedJob claimed, string error, CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        var retryPolicy = _options.DefaultRetry;
        var canRetry = claimed.AttemptCount < claimed.MaxAttempts && retryPolicy.Kind != RetryPolicyKind.NoRetry;

        if (!canRetry)
        {
            await _store.SettleAsync(new SettleRequest
            {
                JobId = claimed.JobId,
                AttemptCount = claimed.AttemptCount,
                LeaseToken = claimed.LeaseToken,
                Outcome = SettleOutcome.Dead,
                LastError = error,
                Now = now,
                DeleteOnSuccess = false,
            }, cancellationToken).ConfigureAwait(false);
            return;
        }

        var delay = RetryDelayCalculator.ComputeDelay(retryPolicy, claimed.AttemptCount);
        await _store.SettleAsync(new SettleRequest
        {
            JobId = claimed.JobId,
            AttemptCount = claimed.AttemptCount,
            LeaseToken = claimed.LeaseToken,
            Outcome = SettleOutcome.Retry,
            LastError = error,
            NextEligibleAt = now + delay,
            Now = now,
            DeleteOnSuccess = false,
        }, cancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessRenewalsAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        foreach (var lease in _leases.GetDueRenewals(now))
        {
            var newLeaseExpiresAt = now + _options.LeaseDuration;
            var renewed = await _store.RenewAsync(
                    lease.JobId,
                    lease.AttemptCount,
                    lease.LeaseToken,
                    newLeaseExpiresAt,
                    cancellationToken)
                .ConfigureAwait(false);

            if (renewed)
            {
                var nextRenewAt = now + _options.LeaseRenewInterval;
                _leases.UpdateRenewal(lease.JobId, lease.AttemptCount, lease.LeaseToken, nextRenewAt, newLeaseExpiresAt);
            }
        }
    }

    private DateTimeOffset CapWakeTime(DateTimeOffset wakeAt, DateTimeOffset now)
    {
        var maxWake = now + _options.MaxIdleSleep;
        return wakeAt <= maxWake ? wakeAt : maxWake;
    }

    private async Task WaitForActiveExecutionsAsync(CancellationToken stoppingToken)
    {
        while (Volatile.Read(ref _activeExecutions) > 0)
            await Task.Delay(50, CancellationToken.None).ConfigureAwait(false);
    }
}
