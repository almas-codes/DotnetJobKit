using DotnetJobKit.Abstractions;

namespace DotnetJobKit.Storage;

public sealed partial class InMemoryJobStore
{
    private readonly Dictionary<string, Guid> _outcomeSuccessors = new(StringComparer.Ordinal);

    public async Task<ClaimBatchResult> ClaimBatchAsync(
        IReadOnlyList<string> queues,
        int maxCount,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        var jobs = await ClaimAsync(queues, maxCount, now, leaseDuration, cancellationToken).ConfigureAwait(false);
        DateTimeOffset? next;
        lock (_sync)
            next = GetNextEligibleAtLocked(queues, now);
        return new ClaimBatchResult(jobs, next);
    }

    public Task<IReadOnlyList<LeaseMaintenanceResult>> MaintainLeasesAsync(
        IReadOnlyList<LeaseMaintenanceRequest> requests,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var results = new List<LeaseMaintenanceResult>(requests.Count);
        lock (_sync)
        {
            foreach (var req in requests)
            {
                if (!_jobs.TryGetValue(req.JobId, out var job))
                {
                    results.Add(new LeaseMaintenanceResult(req.JobId, req.AttemptCount, false, false, null));
                    continue;
                }

                if (!Owns(job, req.AttemptCount, req.LeaseToken))
                {
                    results.Add(new LeaseMaintenanceResult(req.JobId, req.AttemptCount, false, false, null));
                    continue;
                }

                job.EligibleAt = req.NewLeaseExpiry;
                results.Add(new LeaseMaintenanceResult(
                    req.JobId,
                    req.AttemptCount,
                    true,
                    job.CancellationRequested,
                    req.NewLeaseExpiry));
            }
        }

        return Task.FromResult<IReadOnlyList<LeaseMaintenanceResult>>(results);
    }

    public Task<CommitOutcomeResult> CommitOutcomeAsync(
        CommitOutcomeRequest request,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var outcomeKey = OutcomeKey(request.JobId, request.AttemptCount);

        lock (_sync)
        {
            if (_outcomeSuccessors.TryGetValue(outcomeKey, out var existingSuccessor))
            {
                return Task.FromResult(new CommitOutcomeResult(CommitOutcomeStatus.AlreadyCommitted, existingSuccessor));
            }

            if (!_jobs.TryGetValue(request.JobId, out var job))
                return Task.FromResult(new CommitOutcomeResult(CommitOutcomeStatus.StaleOwner, null));

            if (!Owns(job, request.AttemptCount, request.LeaseToken))
                return Task.FromResult(new CommitOutcomeResult(CommitOutcomeStatus.StaleOwner, null));

            Guid? successorId = null;
            if (request.Outcome.Continuation is { } cont)
                successorId = InsertSuccessorLocked(cont, now, outcomeKey);

            switch (request.Outcome.Kind)
            {
                case ExecutionOutcomeKind.Succeeded:
                    ApplyTerminalSuccess(job, request.Outcome, now);
                    break;
                case ExecutionOutcomeKind.Recurring:
                    job.State = JobState.Ready;
                    job.AttemptCount = 0;
                    job.EligibleAt = request.Outcome.NextDueAt ?? now;
                    job.CompletedAt = null;
                    job.LastError = null;
                    job.LeaseToken = null;
                    job.CancellationRequested = false;
                    break;
                case ExecutionOutcomeKind.Retry:
                    job.State = JobState.Ready;
                    job.EligibleAt = request.Outcome.NextDueAt ?? now;
                    job.LastError = request.Outcome.Error;
                    job.CancellationRequested = false;
                    job.LeaseToken = null;
                    break;
                case ExecutionOutcomeKind.Dead:
                case ExecutionOutcomeKind.Cancelled:
                    job.State = request.Outcome.Kind == ExecutionOutcomeKind.Dead ? JobState.Dead : JobState.Cancelled;
                    job.EligibleAt = null;
                    job.CompletedAt = now;
                    job.LastError = request.Outcome.Error;
                    job.LeaseToken = null;
                    break;
            }

            _outcomeSuccessors[outcomeKey] = successorId ?? Guid.Empty;

            return Task.FromResult(new CommitOutcomeResult(CommitOutcomeStatus.Committed, successorId));
        }
    }

    public Task<int> RecoverExhaustedLeasesBatchAsync(
        IReadOnlyList<string> queues,
        int batchSize,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var queueSet = queues.ToHashSet(StringComparer.Ordinal);
        var recovered = 0;
        lock (_sync)
        {
            foreach (var job in _jobs.Values.OrderBy(j => j.EligibleAt).ThenBy(j => j.JobId))
            {
                if (recovered >= batchSize)
                    break;
                if (!queueSet.Contains(job.Queue))
                    continue;
                if (job.State != JobState.Leased || job.EligibleAt is null || job.EligibleAt > now)
                    continue;
                if (job.AttemptCount < job.MaxAttempts)
                    continue;

                job.State = JobState.Dead;
                job.EligibleAt = null;
                job.CompletedAt = now;
                job.LastError ??= "Max attempts exhausted after lease expiration.";
                recovered++;
            }
        }

        return Task.FromResult(recovered);
    }

    private void ApplyTerminalSuccess(MutableJob job, ExecutionOutcome outcome, DateTimeOffset now)
    {
        if (outcome.DeleteOnSuccess)
        {
            _jobs.Remove(job.JobId);
            RemoveIdempotency(job);
            return;
        }

        job.State = JobState.Succeeded;
        job.EligibleAt = null;
        job.CompletedAt = now;
        job.LastError = null;
        job.LeaseToken = null;
    }

    private Guid InsertSuccessorLocked(ContinuationSpec cont, DateTimeOffset now, string outcomeKey)
    {
        var successorId = Guid.NewGuid();
        var successor = new MutableJob
        {
            JobId = successorId,
            Queue = cont.Queue,
            ContractName = cont.ContractName,
            ContractVersion = cont.ContractVersion,
            Payload = cont.Payload,
            State = JobState.Ready,
            EligibleAt = now,
            AttemptCount = 0,
            MaxAttempts = 3,
            CancellationRequested = false,
            CreatedAt = now,
            IdempotencyKey = outcomeKey,
        };
        _jobs[successorId] = successor;
        if (!string.IsNullOrWhiteSpace(outcomeKey))
            _idempotencyIndex[outcomeKey] = successorId;
        return successorId;
    }

    private static string OutcomeKey(Guid jobId, int attemptCount) => $"djk:outcome:{jobId:N}:{attemptCount}";

    private DateTimeOffset? GetNextEligibleAtLocked(IReadOnlyList<string> queues, DateTimeOffset now)
    {
        var queueSet = queues.ToHashSet(StringComparer.Ordinal);
        DateTimeOffset? next = null;
        foreach (var job in _jobs.Values)
        {
            if (!queueSet.Contains(job.Queue))
                continue;
            if (job.State is not (JobState.Ready or JobState.Leased))
                continue;
            if (job.EligibleAt is not { } eligible)
                continue;
            next = next is null || eligible < next ? eligible : next;
        }

        return next;
    }
}
