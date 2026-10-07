using DotnetJobKit.Abstractions;

namespace DotnetJobKit.Handlers;

public delegate Task JobHandleInvoker(
    object handler,
    object job,
    JobContext context,
    CancellationToken cancellationToken);
