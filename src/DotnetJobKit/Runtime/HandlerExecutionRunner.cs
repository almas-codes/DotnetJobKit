using System.Reflection;
using DotnetJobKit.Abstractions;
using DotnetJobKit.Handlers;

namespace DotnetJobKit.Runtime;

internal static class HandlerExecutionRunner
{
    public static async Task RunAsync(
        IJobStore store,
        ClaimedJob claimed,
        object handler,
        Type jobType,
        object job,
        JobContext context,
        TimeSpan cancellationPollInterval,
        TimeSpan leaseDuration,
        TimeSpan leaseRenewInterval,
        TimeProvider timeProvider,
        CancellationToken executionToken,
        CancellationToken stoppingToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(executionToken, stoppingToken);
        var token = linked.Token;

        Task? pollTask = null;
        if (cancellationPollInterval > TimeSpan.Zero)
        {
            pollTask = PollCancellationAsync(store, claimed, cancellationPollInterval, linked, stoppingToken);
        }

        Task? renewTask = null;
        if (leaseRenewInterval > TimeSpan.Zero && leaseDuration > TimeSpan.Zero)
        {
            renewTask = PollLeaseRenewalAsync(
                store,
                claimed,
                leaseDuration,
                leaseRenewInterval,
                timeProvider,
                linked,
                stoppingToken);
        }

        try
        {
            await InvokeHandlerAsync(handler, jobType, job, context, token).ConfigureAwait(false);
        }
        finally
        {
            linked.Cancel();
            if (pollTask is not null)
            {
                try
                {
                    await pollTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }

            if (renewTask is not null)
            {
                try
                {
                    await renewTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
    }

    private static async Task PollLeaseRenewalAsync(
        IJobStore store,
        ClaimedJob claimed,
        TimeSpan leaseDuration,
        TimeSpan interval,
        TimeProvider timeProvider,
        CancellationTokenSource executionCts,
        CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(interval);
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            if (executionCts.IsCancellationRequested)
                return;

            var now = timeProvider.GetUtcNow();
            var newLeaseExpiresAt = now + leaseDuration;
            var renewed = await store.RenewAsync(
                    claimed.JobId,
                    claimed.AttemptCount,
                    claimed.LeaseToken,
                    newLeaseExpiresAt,
                    stoppingToken)
                .ConfigureAwait(false);

            if (!renewed)
            {
                executionCts.Cancel();
                return;
            }
        }
    }

    private static async Task PollCancellationAsync(
        IJobStore store,
        ClaimedJob claimed,
        TimeSpan interval,
        CancellationTokenSource executionCts,
        CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(interval);
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            if (executionCts.IsCancellationRequested)
                return;

            var requested = await store.IsCancellationRequestedAsync(
                claimed.JobId,
                claimed.AttemptCount,
                claimed.LeaseToken,
                stoppingToken).ConfigureAwait(false);

            if (requested)
            {
                executionCts.Cancel();
                return;
            }
        }
    }

    private static async Task InvokeHandlerAsync(
        object handler,
        Type jobType,
        object job,
        JobContext context,
        CancellationToken cancellationToken)
    {
        var handlerInterface = handler.GetType()
            .GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IJobHandler<>));

        if (handlerInterface is null)
            throw new InvalidOperationException($"Handler '{handler.GetType().Name}' does not implement IJobHandler<T>.");

        var method = handlerInterface.GetMethod(
            nameof(IJobHandler<object>.HandleAsync),
            BindingFlags.Public | BindingFlags.Instance);

        if (method is null)
            throw new InvalidOperationException($"Handler '{handler.GetType().Name}' does not expose HandleAsync.");

        var taskObj = method.Invoke(handler, [job, context, cancellationToken]);
        if (taskObj is Task task)
            await task.ConfigureAwait(false);
        else
            throw new InvalidOperationException("HandleAsync must return Task.");
    }
}
