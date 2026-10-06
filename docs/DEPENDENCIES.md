# Dependencies

## DotnetJobKit

| Package | Reason | Removable? |
|---|---|---|
| Microsoft.Extensions.Hosting.Abstractions | `BackgroundService` coordinator | No |
| Microsoft.Extensions.DependencyInjection.Abstractions | DI registration | No |
| Microsoft.Extensions.Logging.Abstractions | Runtime logging | No |
| Microsoft.Extensions.Options | `DotnetJobKitOptions` | No |

Transitive: minimal BCL + abstractions only.

## DotnetJobKit.Sqlite

| Package | Reason | Removable? |
|---|---|---|
| Microsoft.Data.Sqlite | Durable V1 provider | Replace when adding other providers |
| Microsoft.Extensions.DependencyInjection.Abstractions | `AddDotnetJobKitSqlite` | No |
| Microsoft.Extensions.Options | Connection options | No |

## DotnetJobKit.EntityFrameworkCore (optional)

| Package | Reason |
|---|---|
| Microsoft.EntityFrameworkCore.Relational | Transactional enqueue via `DbContext` |

Core packages still avoid Dapper, Hangfire, Polly, and Newtonsoft.Json.
