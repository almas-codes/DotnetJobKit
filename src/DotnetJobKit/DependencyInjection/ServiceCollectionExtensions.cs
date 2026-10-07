using DotnetJobKit.Abstractions;
using DotnetJobKit.Client;
using DotnetJobKit.Configuration;
using DotnetJobKit.Handlers;
using DotnetJobKit.Execution;
using DotnetJobKit.Runtime;
using DotnetJobKit.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DotnetJobKit.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDotnetJobKit(
        this IServiceCollection services,
        Action<DotnetJobKitOptions>? configure = null)
    {
        if (configure is not null)
            services.Configure(configure);
        else
            services.AddOptions<DotnetJobKitOptions>();

        services.TryAddSingleton<TimeProvider>(TimeProvider.System);
        services.TryAddSingleton<IJobWakeSignal, JobWakeSignal>();
        services.TryAddSingleton<JobHandlerRegistry>(sp =>
        {
            var registrations = sp.GetServices<JobContractRegistration>().ToList();
            return new JobHandlerRegistry(registrations);
        });

        services.TryAddSingleton<IJobSubmitter, JobSubmitter>();
        services.AddSingleton<JobExecutionKernel>();
        services.AddHostedService<JobRuntimeCoordinator>();
        services.AddHostedService<RetentionMaintenanceService>();
        return services;
    }

    public static IServiceCollection AddDotnetJobKitInMemoryStorage(
        this IServiceCollection services,
        Action<InMemoryJobStoreOptions>? configure = null)
    {
        if (configure is not null)
            services.Configure(configure);

        services.TryAddSingleton<IJobStore, InMemoryJobStore>();
        return services;
    }

    public static IServiceCollection AddDotnetJobHandler<TJob, THandler>(
        this IServiceCollection services,
        string contract,
        string queue = "default",
        int contractVersion = 1)
        where TJob : notnull
        where THandler : class, IJobHandler<TJob>
    {
        services.AddSingleton(new JobContractRegistration
        {
            JobType = typeof(TJob),
            HandlerType = typeof(THandler),
            ContractName = contract,
            ContractVersion = contractVersion,
            DefaultQueue = queue,
            Invoker = JobHandleInvokerFactory.Create(typeof(TJob)),
        });

        services.AddScoped<THandler>();
        services.AddScoped(typeof(IJobHandler<TJob>), sp => sp.GetRequiredService<THandler>());
        return services;
    }
}
