using DotnetJobKit.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DotnetJobKit.MySql;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDotnetJobKitMySql(
        this IServiceCollection services,
        string connectionString,
        Action<MySqlJobStoreOptions>? configure = null)
    {
        services.Configure<MySqlJobStoreOptions>(options =>
        {
            options.ConnectionString = connectionString;
            configure?.Invoke(options);
        });
        services.TryAddSingleton<IJobStore, MySqlJobStore>();
        return services;
    }
}
