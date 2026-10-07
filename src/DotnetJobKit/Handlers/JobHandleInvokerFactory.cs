using System.Reflection;
using DotnetJobKit.Abstractions;

namespace DotnetJobKit.Handlers;

internal static class JobHandleInvokerFactory
{
    public static JobHandleInvoker Create(Type jobType)
    {
        var handlerInterface = typeof(IJobHandler<>).MakeGenericType(jobType);
        var method = handlerInterface.GetMethod(
            nameof(IJobHandler<object>.HandleAsync),
            BindingFlags.Public | BindingFlags.Instance)
            ?? throw new InvalidOperationException($"IJobHandler<{jobType.Name}> missing HandleAsync.");

        return (handler, job, context, cancellationToken) =>
        {
            var taskObj = method.Invoke(handler, [job, context, cancellationToken]);
            if (taskObj is not Task task)
                throw new InvalidOperationException("HandleAsync must return Task.");
            return task;
        };
    }
}
