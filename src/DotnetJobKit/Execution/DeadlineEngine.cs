namespace DotnetJobKit.Execution;

internal sealed class DeadlineEngine
{
    private readonly PriorityQueue<DeadlineEvent, DateTimeOffset> _queue = new();
    private readonly Dictionary<Guid, long> _generation = new();
    private readonly object _sync = new();

    public void Schedule(DeadlineEvent evt)
    {
        lock (_sync)
        {
            _queue.Enqueue(evt, evt.DueAt);
            ExecutionKernelDiagnostics.DeadlineEventsScheduled++;
        }
    }

    public long BumpGeneration(Guid jobId)
    {
        lock (_sync)
        {
            if (!_generation.TryGetValue(jobId, out var gen))
                gen = 0;
            gen++;
            _generation[jobId] = gen;
            return gen;
        }
    }

    public void Invalidate(Guid jobId) => BumpGeneration(jobId);

    public DateTimeOffset? PeekNextDueUtc()
    {
        lock (_sync)
        {
            while (_queue.Count > 0)
            {
                if (!_queue.TryPeek(out _, out var due))
                    return null;
                return due;
            }

            return null;
        }
    }

    public IReadOnlyList<DeadlineEvent> DrainDue(DateTimeOffset now)
    {
        var due = new List<DeadlineEvent>();
        lock (_sync)
        {
            while (_queue.TryPeek(out var evt, out var dueAt) && dueAt <= now)
            {
                _queue.Dequeue();
                if (_generation.TryGetValue(evt.JobId, out var current) && current != evt.Generation)
                {
                    ExecutionKernelDiagnostics.StaleDeadlineEventsDiscarded++;
                    continue;
                }

                ExecutionKernelDiagnostics.DeadlineEventsProcessed++;
                due.Add(evt);
            }
        }

        return due;
    }
}
