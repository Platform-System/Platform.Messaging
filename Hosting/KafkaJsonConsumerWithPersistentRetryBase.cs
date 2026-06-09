using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Platform.Messaging.Abstractions;
using Platform.Messaging.Configurations;
using Platform.Messaging.Helpers;
using Platform.Messaging.Models;

namespace Platform.Messaging.Hosting;

public abstract class KafkaJsonConsumerWithPersistentRetryBase<TMessage, TOptions> : KafkaConsumerWithRetryBase<TMessage>
    where TMessage : class, new()
    where TOptions : KafkaConsumerTopicRetryOptions
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    protected KafkaJsonConsumerWithPersistentRetryBase(
        IServiceScopeFactory scopeFactory,
        IOptions<TOptions> consumerOptions,
        IKafkaConsumerFactory consumerFactory,
        IKafkaMessagePublisher publisher,
        ILogger logger)
        : base(
            consumerOptions.Value.Topic,
            consumerOptions.Value.GroupId,
            consumerOptions.Value.MaxRetryCount.GetValueOrDefault(),
            consumerFactory,
            publisher,
            logger)
    {
        ScopeFactory = scopeFactory;
        ConsumerOptions = consumerOptions.Value;
    }

    protected IServiceScopeFactory ScopeFactory { get; }
    protected TOptions ConsumerOptions { get; }
    protected static JsonSerializerOptions SerializerOptions => JsonOptions;

    protected sealed override KafkaMessageContext<TMessage>? DeserializeMessage(ConsumeResult<string, string> consumeResult)
    {
        if (string.IsNullOrWhiteSpace(consumeResult.Message.Value))
            return null;

        var message = JsonSerializer.Deserialize<TMessage>(consumeResult.Message.Value, JsonOptions);
        return message is null
            ? null
            : new KafkaMessageContext<TMessage>(
                message,
                0,
                null,
                consumeResult.Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value);
    }

    protected sealed override KafkaRetryEnvelope<TMessage> CreateInvalidMessageEnvelope(ConsumeResult<string, string> consumeResult)
    {
        var now = DateTime.UtcNow;
        return KafkaEnvelopeFactory.CreateInvalidRetryEnvelope(
            new TMessage(),
            consumeResult.Topic,
            InvalidPayloadErrorMessage,
            now,
            now,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);
    }

    protected sealed override async Task PublishDeadLetterAsync(
        ConsumeResult<string, string>? consumeResult,
        KafkaRetryEnvelope<TMessage> envelope,
        CancellationToken cancellationToken)
    {
        envelope.NextAttemptAt = null;

        var key = consumeResult?.Message.Key;
        if (string.IsNullOrWhiteSpace(key))
            key = GetMessageKey(envelope.Payload);

        await Publisher.PublishAsync(
            ConsumerOptions.DeadLetterTopic,
            key ?? Guid.NewGuid().ToString("N"),
            envelope,
            cancellationToken);
    }

    protected sealed override KafkaRetryEnvelope<TMessage> CreateRetryEnvelope(
        KafkaMessageContext<TMessage> context,
        string error,
        int retryCount)
    {
        var failedAt = DateTime.UtcNow;
        return KafkaEnvelopeFactory.CreateRetryEnvelope(
            context,
            error,
            retryCount,
            failedAt,
            failedAt.Add(RetryDelayCalculator.Calculate(
                retryCount,
                ConsumerOptions.BaseRetryDelaySeconds.GetValueOrDefault(),
                ConsumerOptions.MaxRetryDelaySeconds.GetValueOrDefault())),
            message => GetOccurredAt(message, failedAt));
    }

    protected abstract string InvalidPayloadErrorMessage { get; }
    protected abstract string? GetMessageKey(TMessage message);
    protected virtual DateTime GetOccurredAt(TMessage message, DateTime failedAt) => failedAt;
}
