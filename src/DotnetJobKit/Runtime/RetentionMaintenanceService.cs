using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.Runtime;

public sealed class RetentionMaintenanceService : BackgroundService
{
    private readonly IJobStore _store;
    private readonly TimeProvider _timeProvider;
    private readonly DotnetJobKitOptions _options;
    private readonly ILogger<RetentionMaintenanceService> _logger;

    public RetentionMaintenanceService(
        IJobStore store,
        TimeProvider timeProvider,
        IOptions<DotnetJobKitOptions> options,
        ILogger<RetentionMaintenanceService> logger)
    {
        _store = store;
        _timeProvider = timeProvider;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = _options.ReconciliationInterval <= TimeSpan.Zero
            ? TimeSpan.FromSeconds(5)
            : _options.ReconciliationInterval;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
                var now = _timeProvider.GetUtcNow();

                await _store.RecoverExhaustedLeasesBatchAsync(
                        _options.Queues,
                        _options.Retention.PurgeBatchSize,
                        now,
                        stoppingToken)
                    .ConfigureAwait(false);

                var retention = new JobRetentionPurge
                {
                    SucceededRetention = _options.Retention.SucceededRetention,
                    FailedRetention = _options.Retention.FailedRetention,
                    CancelledRetention = _options.Retention.CancelledRetention,
                };

                await _store.DeleteTerminalBatchAsync(
                        now,
                        retention,
                        _options.Retention.PurgeBatchSize,
                        stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Retention/recovery maintenance failed");
            }
        }
    }
}
