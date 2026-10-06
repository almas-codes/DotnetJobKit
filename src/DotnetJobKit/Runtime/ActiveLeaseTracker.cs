namespace DotnetJobKit.Runtime;

internal sealed class ActiveLeaseTracker
{
    private readonly object _sync = new();
    private readonly Dictionary<Guid, ActiveLease> _leases = new();
    private DateTimeOffset _nextRenewAt = DateTimeOffset.MaxValue;

    public void Add(Guid jobId, int attemptCount, Guid? leaseToken, DateTimeOffset renewAt, DateTimeOffset leaseExpiresAt)
    {
        lock (_sync)
        {
            _leases[jobId] = new ActiveLease(jobId, attemptCount, leaseToken, renewAt, leaseExpiresAt);
            if (renewAt < _nextRenewAt)
                _nextRenewAt = renewAt;
        }
    }

    public void Remove(Guid jobId)
    {
        lock (_sync)
        {
            if (_leases.Remove(jobId))
                RecomputeNextRenewAt();
        }
    }

    public DateTimeOffset GetNextRenewAt()
    {
        lock (_sync)
            return _nextRenewAt;
    }

    public IReadOnlyList<ActiveLease> GetDueRenewals(DateTimeOffset now)
    {
        lock (_sync)
            return _leases.Values.Where(l => l.RenewAt <= now).ToList();
    }

    public void UpdateRenewal(Guid jobId, int attemptCount, Guid? leaseToken, DateTimeOffset renewAt, DateTimeOffset leaseExpiresAt)
    {
        lock (_sync)
        {
            _leases[jobId] = new ActiveLease(jobId, attemptCount, leaseToken, renewAt, leaseExpiresAt);
            if (renewAt < _nextRenewAt)
                _nextRenewAt = renewAt;
        }
    }

    public bool TryGet(Guid jobId, out ActiveLease lease)
    {
        lock (_sync)
            return _leases.TryGetValue(jobId, out lease!);
    }

    private void RecomputeNextRenewAt()
    {
        _nextRenewAt = _leases.Count == 0
            ? DateTimeOffset.MaxValue
            : _leases.Values.Min(l => l.RenewAt);
    }

    internal sealed record ActiveLease(
        Guid JobId,
        int AttemptCount,
        Guid? LeaseToken,
        DateTimeOffset RenewAt,
        DateTimeOffset LeaseExpiresAt);
}
