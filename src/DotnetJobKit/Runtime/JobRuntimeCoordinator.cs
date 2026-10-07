using DotnetJobKit.Execution;
using Microsoft.Extensions.Hosting;

namespace DotnetJobKit.Runtime;

public sealed class JobRuntimeCoordinator : BackgroundService
{
    private readonly JobExecutionKernel _kernel;

    public JobRuntimeCoordinator(JobExecutionKernel kernel)
    {
        _kernel = kernel;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        _kernel.RunAsync(stoppingToken);
}
