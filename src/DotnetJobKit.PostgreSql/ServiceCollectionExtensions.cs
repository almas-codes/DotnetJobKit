using DotnetJobKit.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DotnetJobKit.PostgreSql;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDotnetJobKitPostgreSql(
        this IServiceCollection services,
        string connectionString,
        Action<PostgreSqlJobStoreOptions>? configure = null)
    {
        services.Configure<PostgreSqlJobStoreOptions>(options =>
        {
            options.ConnectionString = connectionString;
            configure?.Invoke(options);
        });
        services.TryAddSingleton<IJobStore, PostgreSqlJobStore>();
        return services;
    }
}
