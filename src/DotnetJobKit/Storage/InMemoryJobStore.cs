using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.Storage;

public sealed class InMemoryJobStore : IJobStore
{
    private readonly object _sync = new();
    private readonly Dictionary<Guid, MutableJob> _jobs = new();
    private readonly Dictionary<string, Guid> _idempotencyIndex = new(StringComparer.Ordinal);
    private readonly OwnershipFencingMode _ownershipFencing;

    public InMemoryJobStore(IOptions<InMemoryJobStoreOptions>? options = null)
    {
        _ownershipFencing = options?.Value.OwnershipFencing ?? OwnershipFencingMode.AttemptCount;
    }

    public Task<Guid> SubmitAsync(JobSubmitRequest request, DateTimeOffset now, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            if (!string.IsNullOrWhiteSpace(request.IdempotencyKey))
            {
                if (_idempotencyIndex.TryGetValue(request.IdempotencyKey, out var existingId)
                    && _jobs.TryGetValue(existingId, out var existing)
                    && existing.State is JobState.Ready or JobState.Leased
                    && (existing.IdempotencyExpiresAt is null || existing.IdempotencyExpiresAt > now))
                {
                    return Task.FromResult(existingId);
                }
            }

            var jobId = Guid.NewGuid();
            var eligibleAt = request.EligibleAt ?? now;
            var maxAttempts = request.MaxAttempts ?? 3;
            var expiresAt = request.IdempotencyTtl is { } ttl ? now + ttl : (DateTimeOffset?)null;

            var job = new MutableJob
            {
                JobId = jobId,
                Queue = request.Queue,
                ContractName = request.ContractName,
                ContractVersion = request.ContractVersion,
                Payload = request.Payload,
                State = JobState.Ready,
                EligibleAt = eligibleAt,
                AttemptCount = 0,
                MaxAttempts = maxAttempts,
                CancellationRequested = false,
                CreatedAt = now,
                CompletedAt = null,
                LastError = null,
                IdempotencyKey = request.IdempotencyKey,
                IdempotencyExpiresAt = expiresAt,
                RecurrenceCron = request.RecurrenceCron,
                ContinuationQueue = request.ContinuationQueue,
                ContinuationContractName = request.ContinuationContractName,
                ContinuationContractVersion = request.ContinuationContractVersion,
                ContinuationPayload = request.ContinuationPayload,
            };

            _jobs[jobId] = job;
            if (!string.IsNullOrWhiteSpace(request.IdempotencyKey))
                _idempotencyIndex[request.IdempotencyKey] = jobId;

            return Task.FromResult(jobId);
        }
    }

    public Task<IReadOnlyList<ClaimedJob>> ClaimAsync(
        IReadOnlyList<string> queues,
        int maxCount,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var queueSet = queues.ToHashSet(StringComparer.Ordinal);
        var claimed = new List<ClaimedJob>(Math.Min(maxCount, 4));

        lock (_sync)
        {
            var candidates = _jobs.Values
                .Where(j => queueSet.Contains(j.Queue))
                .Where(j => IsClaimEligible(j, now))
                .OrderBy(j => j.EligibleAt)
                .ThenBy(j => j.JobId)
                .Take(maxCount * 2)
                .ToList();

            foreach (var job in candidates)
            {
                if (claimed.Count >= maxCount)
                    break;

                if (job.State == JobState.Leased && job.EligibleAt <= now)
                {
                    if (job.AttemptCount >= job.MaxAttempts)
                    {
                        job.State = JobState.Dead;
                        job.EligibleAt = null;
                        job.CompletedAt = now;
                        job.LastError ??= "Max attempts exhausted after lease expiration.";
                        continue;
                    }
                }

                if (!IsClaimEligible(job, now))
                    continue;

                job.AttemptCount++;
                job.State = JobState.Leased;
                var leaseExpiresAt = now + leaseDuration;
                job.EligibleAt = leaseExpiresAt;
                job.LeaseToken = _ownershipFencing == OwnershipFencingMode.LeaseToken ? Guid.NewGuid() : null;

                claimed.Add(new ClaimedJob
                {
                    JobId = job.JobId,
                    Queue = job.Queue,
                    ContractName = job.ContractName,
                    ContractVersion = job.ContractVersion,
                    Payload = job.Payload,
                    AttemptCount = job.AttemptCount,
                    MaxAttempts = job.MaxAttempts,
                    CancellationRequested = job.CancellationRequested,
                    LeaseExpiresAt = leaseExpiresAt,
                    LeaseToken = job.LeaseToken,
                    RecurrenceCron = job.RecurrenceCron,
                    ContinuationQueue = job.ContinuationQueue,
                    ContinuationContractName = job.ContinuationContractName,
                    ContinuationContractVersion = job.ContinuationContractVersion,
                    ContinuationPayload = job.ContinuationPayload,
                });
            }
        }

        return Task.FromResult<IReadOnlyList<ClaimedJob>>(claimed);
    }

    public Task<bool> RenewAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            if (!_jobs.TryGetValue(jobId, out var job))
                return Task.FromResult(false);

            if (!Owns(job, attemptCount, leaseToken))
                return Task.FromResult(false);

            job.EligibleAt = leaseExpiresAt;
            return Task.FromResult(true);
        }
    }

    public Task<bool> IsCancellationRequestedAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            if (!_jobs.TryGetValue(jobId, out var job))
                return Task.FromResult(false);

            if (!Owns(job, attemptCount, leaseToken))
                return Task.FromResult(false);

            return Task.FromResult(job.CancellationRequested);
        }
    }

    public Task<bool> SettleAsync(SettleRequest request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            if (!_jobs.TryGetValue(request.JobId, out var job))
                return Task.FromResult(false);

            if (!Owns(job, request.AttemptCount, request.LeaseToken))
                return Task.FromResult(false);

            switch (request.Outcome)
            {
                case SettleOutcome.Succeeded:
                    if (request.DeleteOnSuccess)
                    {
                        _jobs.Remove(job.JobId);
                        RemoveIdempotency(job);
                    }
                    else
                    {
                        job.State = JobState.Succeeded;
                        job.EligibleAt = null;
                        job.CompletedAt = request.Now;
                        job.LastError = null;
                    }

                    break;

                case SettleOutcome.Retry:
                    job.State = JobState.Ready;
                    job.EligibleAt = request.NextEligibleAt ?? request.Now;
                    job.LastError = request.LastError;
                    job.CancellationRequested = false;
                    job.LeaseToken = null;
                    break;

                case SettleOutcome.Dead:
                    job.State = JobState.Dead;
                    job.EligibleAt = null;
                    job.CompletedAt = request.Now;
                    job.LastError = request.LastError;
                    break;

                case SettleOutcome.Cancelled:
                    job.State = JobState.Cancelled;
                    job.EligibleAt = null;
                    job.CompletedAt = request.Now;
                    break;
            }

            return Task.FromResult(true);
        }
    }

    public Task<bool> CancelReadyAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            if (!_jobs.TryGetValue(jobId, out var job))
                return Task.FromResult(false);

            if (job.State != JobState.Ready)
                return Task.FromResult(false);

            job.State = JobState.Cancelled;
            job.EligibleAt = null;
            job.CompletedAt = now;
            return Task.FromResult(true);
        }
    }

    public Task<bool> RequestLeasedCancellationAsync(Guid jobId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            if (!_jobs.TryGetValue(jobId, out var job))
                return Task.FromResult(false);

            if (job.State != JobState.Leased)
                return Task.FromResult(false);

            job.CancellationRequested = true;
            return Task.FromResult(true);
        }
    }

    public Task<JobRecord?> GetAsync(Guid jobId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            return Task.FromResult(_jobs.TryGetValue(jobId, out var job) ? job.ToRecord() : null);
        }
    }

    public Task<DateTimeOffset?> GetNextEligibleAtAsync(
        IReadOnlyList<string> queues,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var queueSet = queues.ToHashSet(StringComparer.Ordinal);

        lock (_sync)
        {
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

            return Task.FromResult(next);
        }
    }

    public Task<int> DeleteTerminalBatchAsync(
        DateTimeOffset now,
        JobRetentionPurge retention,
        int batchSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            var toDelete = _jobs.Values
                .Where(j => j.CompletedAt is not null && IsExpired(j, now, retention))
                .Take(batchSize)
                .Select(j => j.JobId)
                .ToList();

            foreach (var id in toDelete)
            {
                if (_jobs.Remove(id, out var job))
                    RemoveIdempotency(job);
            }

            return Task.FromResult(toDelete.Count);
        }
    }

    public Task<IReadOnlyDictionary<JobState, int>> GetCountsByStateAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            var counts = Enum.GetValues<JobState>().ToDictionary(s => s, _ => 0);
            foreach (var job in _jobs.Values)
                counts[job.State]++;
            return Task.FromResult<IReadOnlyDictionary<JobState, int>>(counts);
        }
    }

    public Task<IReadOnlyList<JobRecord>> ListJobsAsync(
        JobState? state,
        string? queue,
        int limit,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            var query = _jobs.Values.AsEnumerable();
            if (state is not null)
                query = query.Where(j => j.State == state);
            if (!string.IsNullOrWhiteSpace(queue))
                query = query.Where(j => j.Queue == queue);

            var list = query
                .OrderByDescending(j => j.CreatedAt)
                .Take(Math.Max(1, limit))
                .Select(j => j.ToRecord())
                .ToList();
            return Task.FromResult<IReadOnlyList<JobRecord>>(list);
        }
    }

    public Task<bool> RequeueDeadAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            if (!_jobs.TryGetValue(jobId, out var job) || job.State != JobState.Dead)
                return Task.FromResult(false);

            job.State = JobState.Ready;
            job.EligibleAt = now;
            job.AttemptCount = 0;
            job.CompletedAt = null;
            job.LastError = null;
            job.LeaseToken = null;
            job.CancellationRequested = false;
            return Task.FromResult(true);
        }
    }

    public Task<bool> DeleteTerminalJobAsync(Guid jobId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            if (!_jobs.TryGetValue(jobId, out var job))
                return Task.FromResult(false);
            if (job.State is not (JobState.Dead or JobState.Cancelled or JobState.Succeeded))
                return Task.FromResult(false);

            _jobs.Remove(jobId);
            RemoveIdempotency(job);
            return Task.FromResult(true);
        }
    }

    public Task<bool> CompleteAsRecurringReadyAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        DateTimeOffset nextEligibleAt,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            if (!_jobs.TryGetValue(jobId, out var job) || !Owns(job, attemptCount, leaseToken))
                return Task.FromResult(false);

            job.State = JobState.Ready;
            job.AttemptCount = 0;
            job.EligibleAt = nextEligibleAt;
            job.CompletedAt = null;
            job.LastError = null;
            job.LeaseToken = null;
            job.CancellationRequested = false;
            return Task.FromResult(true);
        }
    }

    private static bool IsExpired(MutableJob job, DateTimeOffset now, JobRetentionPurge retention)
    {
        if (job.CompletedAt is not { } completed)
            return false;

        return job.State switch
        {
            JobState.Succeeded => completed + retention.SucceededRetention <= now,
            JobState.Dead => completed + retention.FailedRetention <= now,
            JobState.Cancelled => completed + retention.CancelledRetention <= now,
            _ => false,
        };
    }

    private static bool Owns(MutableJob job, int attemptCount, Guid? leaseToken) =>
        job.State == JobState.Leased
        && job.AttemptCount == attemptCount
        && (leaseToken is null || job.LeaseToken == leaseToken);

    private static bool IsClaimEligible(MutableJob job, DateTimeOffset now) =>
        job.EligibleAt is not null
        && job.EligibleAt <= now
        && job.State is JobState.Ready
            or JobState.Leased;

    private void RemoveIdempotency(MutableJob job)
    {
        if (string.IsNullOrWhiteSpace(job.IdempotencyKey))
            return;

        if (_idempotencyIndex.TryGetValue(job.IdempotencyKey, out var id) && id == job.JobId)
            _idempotencyIndex.Remove(job.IdempotencyKey);
    }

    private sealed class MutableJob
    {
        public required Guid JobId { get; init; }
        public required string Queue { get; set; }
        public required string ContractName { get; set; }
        public required int ContractVersion { get; set; }
        public required string Payload { get; set; }
        public required JobState State { get; set; }
        public DateTimeOffset? EligibleAt { get; set; }
        public required int AttemptCount { get; set; }
        public required int MaxAttempts { get; set; }
        public required bool CancellationRequested { get; set; }
        public required DateTimeOffset CreatedAt { get; init; }
        public DateTimeOffset? CompletedAt { get; set; }
        public string? LastError { get; set; }
        public string? IdempotencyKey { get; init; }
        public DateTimeOffset? IdempotencyExpiresAt { get; init; }
        public Guid? LeaseToken { get; set; }
        public string? RecurrenceCron { get; init; }
        public string? ContinuationQueue { get; init; }
        public string? ContinuationContractName { get; init; }
        public int? ContinuationContractVersion { get; init; }
        public string? ContinuationPayload { get; init; }

        public JobRecord ToRecord() => new()
        {
            JobId = JobId,
            Queue = Queue,
            ContractName = ContractName,
            ContractVersion = ContractVersion,
            Payload = Payload,
            State = State,
            EligibleAt = EligibleAt,
            AttemptCount = AttemptCount,
            MaxAttempts = MaxAttempts,
            CancellationRequested = CancellationRequested,
            CreatedAt = CreatedAt,
            CompletedAt = CompletedAt,
            LastError = LastError,
            IdempotencyKey = IdempotencyKey,
            IdempotencyExpiresAt = IdempotencyExpiresAt,
            RecurrenceCron = RecurrenceCron,
            ContinuationQueue = ContinuationQueue,
            ContinuationContractName = ContinuationContractName,
            ContinuationContractVersion = ContinuationContractVersion,
            ContinuationPayload = ContinuationPayload,
        };
    }
}
