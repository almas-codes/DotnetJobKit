using DotnetJobKit.Abstractions;
using DotnetJobKit.Handlers;

namespace DotnetJobKit.Runtime;

internal static class HandlerExecutionRunner
{
    public static Task ExecuteAsync(
        JobHandleInvoker invoker,
        object handler,
        object job,
        JobContext context,
        CancellationToken cancellationToken)
        => invoker(handler, job, context, cancellationToken);
}
