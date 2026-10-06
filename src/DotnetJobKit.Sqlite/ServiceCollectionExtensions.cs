using DotnetJobKit.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DotnetJobKit.Sqlite;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDotnetJobKitSqlite(
        this IServiceCollection services,
        string connectionString,
        Action<SqliteJobStoreOptions>? configure = null)
    {
        services.Configure<SqliteJobStoreOptions>(options =>
        {
            options.ConnectionString = connectionString;
            configure?.Invoke(options);
        });

        services.TryAddSingleton<IJobStore, SqliteJobStore>();
        return services;
    }
}
