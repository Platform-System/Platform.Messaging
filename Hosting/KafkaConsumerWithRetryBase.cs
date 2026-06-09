using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.Messaging.Abstractions;
using Platform.Messaging.Models;

namespace Platform.Messaging.Hosting;

public abstract class KafkaConsumerWithRetryBase<TMessage> : BackgroundService
{
    private readonly string _topic;
    private readonly string _groupId;
    private readonly int _maxRetryCount;
    private readonly IKafkaConsumerFactory _consumerFactory;
    protected readonly IKafkaMessagePublisher Publisher;
    protected readonly ILogger Logger;

    protected KafkaConsumerWithRetryBase(
        string topic,
        string groupId,
        int maxRetryCount,
        IKafkaConsumerFactory consumerFactory,
        IKafkaMessagePublisher publisher,
        ILogger logger)
    {
        _topic = topic;
        _groupId = groupId;
        _maxRetryCount = maxRetryCount;
        _consumerFactory = consumerFactory;
        Publisher = publisher;
        Logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = _consumerFactory.Create(_groupId, _topic);

        Logger.LogInformation(
            "Subscribed to Kafka topic {Topic} with group {GroupId}.",
            _topic,
            _groupId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _ = await ProcessDueRetryMessageAsync(stoppingToken);

                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                if (consumeResult is null)
                    continue;

                if (string.IsNullOrWhiteSpace(consumeResult.Message?.Value))
                {
                    consumer.Commit(consumeResult);
                    continue;
                }

                var messageContext = DeserializeMessage(consumeResult);
                if (messageContext is null || !IsMessageValid(messageContext.Message))
                {
                    await PublishDeadLetterAsync(
                        consumeResult,
                        CreateInvalidMessageEnvelope(consumeResult),
                        stoppingToken);

                    consumer.Commit(consumeResult);
                    continue;
                }

                var result = await ProcessMessageAsync(messageContext.Message, stoppingToken);
                if (!result.IsSuccess)
                {
                    var error = string.Join(", ", result.Errors);
                    Logger.LogWarning("Failed to process Kafka message on topic {Topic}: {Errors}", _topic, error);

                    if (messageContext.RetryCount + 1 < _maxRetryCount)
                    {
                        await StoreRetryAsync(messageContext, error, messageContext.RetryCount + 1, stoppingToken);
                        consumer.Commit(consumeResult);
                        continue;
                    }

                    await PublishDeadLetterAsync(
                        consumeResult,
                        CreateRetryEnvelope(messageContext, error, messageContext.RetryCount),
                        stoppingToken);
                    consumer.Commit(consumeResult);
                    continue;
                }

                consumer.Commit(consumeResult);
                OnMessageProcessed(messageContext.Message);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                Logger.LogError(ex, "Kafka consume error on topic {Topic}.", _topic);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Unexpected Kafka consumer error on topic {Topic}.", _topic);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }

        consumer.Close();
    }

    protected abstract KafkaMessageContext<TMessage>? DeserializeMessage(ConsumeResult<string, string> consumeResult);
    protected abstract bool IsMessageValid(TMessage message);
    protected abstract KafkaRetryEnvelope<TMessage> CreateInvalidMessageEnvelope(ConsumeResult<string, string> consumeResult);
    protected abstract KafkaRetryEnvelope<TMessage> CreateRetryEnvelope(KafkaMessageContext<TMessage> context, string error, int retryCount);
    protected abstract Task<KafkaMessageProcessResult> ProcessMessageAsync(TMessage message, CancellationToken cancellationToken);
    protected abstract Task StoreRetryAsync(KafkaMessageContext<TMessage> context, string error, int retryCount, CancellationToken cancellationToken);
    protected abstract Task<bool> ProcessDueRetryMessageAsync(CancellationToken cancellationToken);
    protected abstract Task PublishDeadLetterAsync(
        ConsumeResult<string, string>? consumeResult,
        KafkaRetryEnvelope<TMessage> envelope,
        CancellationToken cancellationToken);

    protected virtual void OnMessageProcessed(TMessage message)
    {
    }
}
