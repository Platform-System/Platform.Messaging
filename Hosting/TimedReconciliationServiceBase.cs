using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Platform.Messaging.Configurations;

namespace Platform.Messaging.Hosting;

public abstract class TimedReconciliationServiceBase<TOptions> : BackgroundService
    where TOptions : class, IReconciliationOptions
{
    private readonly TOptions _options;
    private readonly ILogger _logger;

    protected TimedReconciliationServiceBase(IOptions<TOptions> options, ILogger logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    protected TOptions Options => _options;
    protected ILogger Logger => _logger;

    protected abstract string ReconciliationName { get; }
    protected abstract Task RunOnceAsync(CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("{ReconciliationName} is disabled.", ReconciliationName);
            return;
        }

        var interval = TimeSpan.FromSeconds(_options.IntervalSeconds);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{ReconciliationName} failed.", ReconciliationName);
            }

            await Task.Delay(interval, stoppingToken);
        }
    }
}
