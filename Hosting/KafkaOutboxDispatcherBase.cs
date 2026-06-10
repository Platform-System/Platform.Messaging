using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.Messaging.Configurations;

namespace Platform.Messaging.Hosting;

public abstract class KafkaOutboxDispatcherBase<TClaimedMessage> : BackgroundService
{
    private readonly int _dispatchIntervalSeconds;
    private readonly int _maxRetryCount;
    protected readonly ILogger Logger;

    protected KafkaOutboxDispatcherBase(
        int dispatchIntervalSeconds,
        int maxRetryCount,
        ILogger logger)
    {
        _dispatchIntervalSeconds = dispatchIntervalSeconds;
        _maxRetryCount = maxRetryCount;
        Logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DispatchBatchAsyncCore(stoppingToken);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "{DispatcherName} dispatch cycle failed.", GetType().Name);
            }

            await Task.Delay(TimeSpan.FromSeconds(_dispatchIntervalSeconds), stoppingToken);
        }
    }

    protected async Task DispatchBatchAsyncCore(CancellationToken cancellationToken)
    {
        var messages = await ClaimDueMessagesAsync(cancellationToken);
        if (messages.Count == 0)
            return;

        foreach (var message in messages)
        {
            try
            {
                await PublishAsync(message, cancellationToken);
                await MarkProcessedAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                var retryCount = GetRetryCount(message) + 1;
                var retryConfiguration = GetRetryConfiguration(message);

                if (retryCount < retryConfiguration.MaxRetryCount)
                {
                    var nextRetryAt = await ScheduleRetryAsync(message, retryCount, ex.Message, cancellationToken);
                    OnRetryScheduled(message, retryCount, nextRetryAt, ex);
                    continue;
                }

                try
                {
                    SetRetryCount(message, retryCount);
                    await PublishDeadLetterAsync(message, ex.Message, cancellationToken);
                    await MarkProcessedAsync(message, cancellationToken);
                }
                catch (Exception deadLetterException)
                {
                    var nextRetryAt = await ScheduleRetryAsync(message, retryCount, deadLetterException.Message, cancellationToken);
                    OnDeadLetterPublishFailed(message, nextRetryAt, deadLetterException);
                }
            }
        }
    }

    protected virtual void OnRetryScheduled(TClaimedMessage message, int retryCount, DateTime nextRetryAt, Exception exception)
    {
    }

    protected virtual OutboxRetryConfiguration GetRetryConfiguration(TClaimedMessage message)
        => OutboxRetryConfiguration.Create(_maxRetryCount);

    protected virtual void OnDeadLetterPublishFailed(TClaimedMessage message, DateTime nextRetryAt, Exception exception)
    {
    }

    protected abstract Task<IReadOnlyCollection<TClaimedMessage>> ClaimDueMessagesAsync(CancellationToken cancellationToken);
    protected abstract int GetRetryCount(TClaimedMessage message);
    protected abstract void SetRetryCount(TClaimedMessage message, int retryCount);
    protected abstract Task PublishAsync(TClaimedMessage message, CancellationToken cancellationToken);
    protected abstract Task PublishDeadLetterAsync(TClaimedMessage message, string error, CancellationToken cancellationToken);
    protected abstract Task MarkProcessedAsync(TClaimedMessage message, CancellationToken cancellationToken);
    protected abstract Task<DateTime> ScheduleRetryAsync(TClaimedMessage message, int retryCount, string error, CancellationToken cancellationToken);
}
