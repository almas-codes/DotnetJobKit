using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Handlers;
using DotnetJobKit.Runtime;
using DotnetJobKit.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.Execution;

public sealed class JobExecutionKernel
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IJobStore _store;
    private readonly JobHandlerRegistry _registry;
    private readonly IJobWakeSignal _wakeSignal;
    private readonly TimeProvider _timeProvider;
    private readonly DotnetJobKitOptions _options;
    private readonly ILogger<JobExecutionKernel> _logger;
    private readonly DeadlineEngine _deadlines = new();
    private readonly Dictionary<Guid, ActiveExecution> _active = new();
    private readonly object _activeSync = new();
    private int _activeCount;
    private DateTimeOffset? _nextWakeHint;

    public JobExecutionKernel(
        IServiceProvider serviceProvider,
        IJobStore store,
        JobHandlerRegistry registry,
        IJobWakeSignal wakeSignal,
        TimeProvider timeProvider,
        IOptions<DotnetJobKitOptions> options,
        ILogger<JobExecutionKernel> logger)
    {
        _serviceProvider = serviceProvider;
        _store = store;
        _registry = registry;
        _wakeSignal = wakeSignal;
        _timeProvider = timeProvider;
        _options = options.Value;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Job execution kernel started (max concurrency {MaxConcurrency})", _options.MaxConcurrency);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var now = _timeProvider.GetUtcNow();
                await ProcessDueDeadlinesAsync(now, stoppingToken).ConfigureAwait(false);
                await ReapCompletedExecutionsAsync().ConfigureAwait(false);

                var available = _options.MaxConcurrency - Volatile.Read(ref _activeCount);
                if (available > 0)
                {
                    var batch = await _store.ClaimBatchAsync(
                            _options.Queues,
                            available,
                            now,
                            _options.LeaseDuration,
                            stoppingToken)
                        .ConfigureAwait(false);

                    if (batch.NextDueAt is { } hint)
                        _nextWakeHint = _nextWakeHint is null ? hint : (_nextWakeHint <= hint ? _nextWakeHint : hint);

                    foreach (var job in batch.Jobs)
                        StartExecution(job, stoppingToken);
                }

                if (Volatile.Read(ref _activeCount) > 0)
                {
                    var next = ComputeNextWake(now);
                    ExecutionKernelDiagnostics.SchedulerWakeups++;
                    await _wakeSignal.WaitUntilAsync(next, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                var wakeAt = ComputeNextWake(now);
                if (_nextWakeHint is null && _deadlines.PeekNextDueUtc() is null && wakeAt >= now.AddYears(10))
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
                _logger.LogError(ex, "Job execution kernel loop failed");
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

        await WaitForActiveExecutionsAsync().ConfigureAwait(false);
        _logger.LogInformation("Job execution kernel stopped");
    }

    private DateTimeOffset ComputeNextWake(DateTimeOffset now)
    {
        var candidates = new List<DateTimeOffset>();
        if (_deadlines.PeekNextDueUtc() is { } d)
            candidates.Add(d);
        if (_nextWakeHint is { } h)
            candidates.Add(h);

        if (candidates.Count == 0)
            return now.AddYears(10);

        var wakeAt = candidates.Min();
        var maxWake = now + _options.MaxIdleSleep;
        return wakeAt <= maxWake ? wakeAt : maxWake;
    }

    private void StartExecution(ClaimedJob claimed, CancellationToken stoppingToken)
    {
        var gen = _deadlines.BumpGeneration(claimed.JobId);
        RegisterLeaseDeadline(claimed, gen);

        if (_options.ExecutionTimeout > TimeSpan.Zero)
        {
            var timeoutAt = _timeProvider.GetUtcNow() + _options.ExecutionTimeout;
            _deadlines.Schedule(new DeadlineEvent(
                claimed.JobId,
                claimed.AttemptCount,
                DeadlineKind.ExecutionTimeout,
                gen,
                timeoutAt));
        }

        var executionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        if (claimed.CancellationRequested)
            executionCts.Cancel();

        var active = new ActiveExecution(claimed, executionCts);
        lock (_activeSync)
        {
            _active[claimed.JobId] = active;
        }

        Interlocked.Increment(ref _activeCount);
        active.ExecutionTask = ExecuteJobAsync(active, stoppingToken);
    }

    private void RegisterLeaseDeadline(ClaimedJob claimed, long gen)
    {
        var maintenanceInterval = ResolveMaintenanceInterval();
        if (maintenanceInterval <= TimeSpan.Zero)
            return;

        var at = _timeProvider.GetUtcNow() + maintenanceInterval;
        _deadlines.Schedule(new DeadlineEvent(
            claimed.JobId,
            claimed.AttemptCount,
            DeadlineKind.LeaseMaintenance,
            gen,
            at));
    }

    private TimeSpan ResolveMaintenanceInterval()
    {
        if (_options.LeaseRenewInterval <= TimeSpan.Zero && _options.CancellationPollInterval <= TimeSpan.Zero)
            return TimeSpan.Zero;

        if (_options.CancellationPollInterval <= TimeSpan.Zero)
            return _options.LeaseRenewInterval;

        if (_options.LeaseRenewInterval <= TimeSpan.Zero)
            return _options.CancellationPollInterval;

        return _options.LeaseRenewInterval < _options.CancellationPollInterval
            ? _options.LeaseRenewInterval
            : _options.CancellationPollInterval;
    }

    private async Task ProcessDueDeadlinesAsync(DateTimeOffset now, CancellationToken stoppingToken)
    {
        var due = _deadlines.DrainDue(now);
        if (due.Count == 0)
            return;

        var maintenance = new List<LeaseMaintenanceRequest>();
        foreach (var evt in due)
        {
            if (evt.Kind == DeadlineKind.ExecutionTimeout)
            {
                lock (_activeSync)
                {
                    if (_active.TryGetValue(evt.JobId, out var timed)
                        && timed.Claimed.AttemptCount == evt.AttemptCount)
                    {
                        timed.ExecutionCts.Cancel();
                    }
                }

                continue;
            }

            if (evt.Kind != DeadlineKind.LeaseMaintenance)
                continue;

            ActiveExecution? active;
            lock (_activeSync)
            {
                _active.TryGetValue(evt.JobId, out active);
            }

            if (active is null || active.Claimed.AttemptCount != evt.AttemptCount)
                continue;

            var newExpiry = now + _options.LeaseDuration;
            maintenance.Add(new LeaseMaintenanceRequest(
                evt.JobId,
                evt.AttemptCount,
                active.Claimed.LeaseToken,
                newExpiry));
        }

        if (maintenance.Count == 0)
            return;

        ExecutionKernelDiagnostics.MaintenanceBatches++;
        ExecutionKernelDiagnostics.MaintenanceJobs += maintenance.Count;
        var results = await _store.MaintainLeasesAsync(maintenance, stoppingToken).ConfigureAwait(false);
        foreach (var result in results)
        {
            if (!result.StillOwner)
            {
                CancelLocalExecution(result.JobId);
                continue;
            }

            if (result.CancellationRequested)
            {
                CancelLocalExecution(result.JobId);
                continue;
            }

            lock (_activeSync)
            {
                if (_active.TryGetValue(result.JobId, out var active)
                    && active.Claimed.AttemptCount == result.AttemptCount)
                {
                    var gen = _deadlines.BumpGeneration(result.JobId);
                    RegisterLeaseDeadline(active.Claimed, gen);
                }
            }
        }
    }

    private void CancelLocalExecution(Guid jobId)
    {
        lock (_activeSync)
        {
            if (_active.TryGetValue(jobId, out var active))
                active.ExecutionCts.Cancel();
        }
    }

    private async Task ExecuteJobAsync(ActiveExecution active, CancellationToken stoppingToken)
    {
        var claimed = active.Claimed;
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var jobObject = _registry.Deserialize(claimed.ContractName, claimed.ContractVersion, claimed.Payload);
            var registration = _registry.GetByContract(claimed.ContractName, claimed.ContractVersion);
            var handler = scope.ServiceProvider.GetRequiredService(registration.HandlerType);
            var invoker = registration.Invoker ?? JobHandleInvokerFactory.Create(registration.JobType);

            var context = new JobContext
            {
                JobId = claimed.JobId,
                Queue = claimed.Queue,
                ContractName = claimed.ContractName,
                AttemptCount = claimed.AttemptCount,
                MaxAttempts = claimed.MaxAttempts,
            };

            await HandlerExecutionRunner.ExecuteAsync(
                invoker,
                handler,
                jobObject,
                context,
                active.ExecutionCts.Token).ConfigureAwait(false);

            var outcome = BuildSuccessOutcome(claimed);
            await CommitAsync(claimed, outcome, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (active.ExecutionCts.IsCancellationRequested && !stoppingToken.IsCancellationRequested)
        {
            var outcome = new ExecutionOutcome(ExecutionOutcomeKind.Cancelled, Error: "Execution cancelled or timed out.");
            await CommitAsync(claimed, outcome, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Job {JobId} ({Contract}) failed on attempt {Attempt}", claimed.JobId, claimed.ContractName, claimed.AttemptCount);
            var outcome = BuildFailureOutcome(claimed, ex.Message);
            await CommitAsync(claimed, outcome, stoppingToken).ConfigureAwait(false);
        }
        finally
        {
            _deadlines.Invalidate(claimed.JobId);
            lock (_activeSync)
            {
                _active.Remove(claimed.JobId);
            }

            Interlocked.Decrement(ref _activeCount);
            _wakeSignal.Notify();
        }
    }

    private ExecutionOutcome BuildSuccessOutcome(ClaimedJob claimed)
    {
        var now = _timeProvider.GetUtcNow();
        if (claimed.RecurrenceCron is { Length: > 0 } cron)
        {
            var next = CronFiveFieldParser.GetNextOccurrence(cron, now);
            return new ExecutionOutcome(
                ExecutionOutcomeKind.Recurring,
                NextDueAt: next,
                RecurrenceCron: cron,
                Continuation: BuildContinuation(claimed),
                DeleteOnSuccess: false);
        }

        return new ExecutionOutcome(
            ExecutionOutcomeKind.Succeeded,
            DeleteOnSuccess: _options.Retention.ShouldDeleteImmediatelyOnSuccess(),
            Continuation: BuildContinuation(claimed));
    }

    private static ContinuationSpec? BuildContinuation(ClaimedJob claimed)
    {
        if (string.IsNullOrWhiteSpace(claimed.ContinuationContractName)
            || string.IsNullOrWhiteSpace(claimed.ContinuationPayload))
        {
            return null;
        }

        return new ContinuationSpec(
            claimed.ContinuationQueue ?? claimed.Queue,
            claimed.ContinuationContractName,
            claimed.ContinuationContractVersion ?? 1,
            claimed.ContinuationPayload);
    }

    private ExecutionOutcome BuildFailureOutcome(ClaimedJob claimed, string error)
    {
        var retryPolicy = _options.DefaultRetry;
        var canRetry = claimed.AttemptCount < claimed.MaxAttempts && retryPolicy.Kind != RetryPolicyKind.NoRetry;
        if (!canRetry)
            return new ExecutionOutcome(ExecutionOutcomeKind.Dead, Error: error);

        var delay = RetryDelayCalculator.ComputeDelay(retryPolicy, claimed.AttemptCount);
        return new ExecutionOutcome(ExecutionOutcomeKind.Retry, NextDueAt: _timeProvider.GetUtcNow() + delay, Error: error);
    }

    private async Task CommitAsync(ClaimedJob claimed, ExecutionOutcome outcome, CancellationToken stoppingToken)
    {
        var request = new CommitOutcomeRequest(
            claimed.JobId,
            claimed.AttemptCount,
            claimed.LeaseToken,
            outcome,
            claimed.MaxAttempts,
            _options.DefaultRetry);

        var result = await _store.CommitOutcomeAsync(request, _timeProvider.GetUtcNow(), CancellationToken.None)
            .ConfigureAwait(false);

        if (result.Status == CommitOutcomeStatus.StaleOwner)
        {
            _logger.LogWarning(
                "Stale owner commit ignored for job {JobId} attempt {Attempt}",
                claimed.JobId,
                claimed.AttemptCount);
            return;
        }

        if (result.Status == CommitOutcomeStatus.Committed)
            _wakeSignal.Notify();
    }

    private async Task ReapCompletedExecutionsAsync()
    {
        List<Task>? tasks = null;
        lock (_activeSync)
        {
            foreach (var active in _active.Values)
            {
                if (active.ExecutionTask is { IsCompleted: true })
                {
                    tasks ??= new List<Task>();
                    tasks.Add(active.ExecutionTask);
                }
            }
        }

        if (tasks is not null)
        {
            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch
            {
                // Individual execution paths log and commit.
            }
        }
    }

    private async Task WaitForActiveExecutionsAsync()
    {
        while (Volatile.Read(ref _activeCount) > 0)
        {
            await ReapCompletedExecutionsAsync().ConfigureAwait(false);
            await Task.Delay(50).ConfigureAwait(false);
        }
    }

    private sealed class ActiveExecution
    {
        public ActiveExecution(ClaimedJob claimed, CancellationTokenSource executionCts)
        {
            Claimed = claimed;
            ExecutionCts = executionCts;
        }

        public ClaimedJob Claimed { get; }
        public CancellationTokenSource ExecutionCts { get; }
        public Task? ExecutionTask { get; set; }
    }
}
