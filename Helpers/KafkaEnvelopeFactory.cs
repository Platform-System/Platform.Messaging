using Platform.Messaging.Models;

namespace Platform.Messaging.Helpers;

public static class KafkaEnvelopeFactory
{
    public static KafkaRetryEnvelope<TMessage> CreateRetryEnvelope<TMessage>(
        KafkaMessageContext<TMessage> context,
        string error,
        int retryCount,
        DateTime failedAt,
        DateTime? nextAttemptAt,
        Func<TMessage, DateTime> occurredAtSelector)
    {
        return new KafkaRetryEnvelope<TMessage>
        {
            Payload = context.Message,
            RetryCount = retryCount,
            SourceTopic = context.SourceTopic,
            LastError = error,
            OccurredAt = occurredAtSelector(context.Message),
            FailedAt = failedAt,
            NextAttemptAt = nextAttemptAt,
            OriginalPartition = context.OriginalPartition,
            OriginalOffset = context.OriginalOffset
        };
    }

    public static KafkaRetryEnvelope<TMessage> CreateInvalidRetryEnvelope<TMessage>(
        TMessage payload,
        string sourceTopic,
        string error,
        DateTime occurredAt,
        DateTime failedAt,
        int? originalPartition,
        long? originalOffset)
    {
        return new KafkaRetryEnvelope<TMessage>
        {
            Payload = payload,
            RetryCount = 0,
            SourceTopic = sourceTopic,
            LastError = error,
            OccurredAt = occurredAt,
            FailedAt = failedAt,
            NextAttemptAt = null,
            OriginalPartition = originalPartition,
            OriginalOffset = originalOffset
        };
    }

    public static TEnvelope CreateDeadLetterEnvelope<TEnvelope, TMessage>(
        TMessage payload,
        string messageType,
        int retryCount,
        string error,
        DateTime occurredAt,
        DateTime failedAt,
        Guid? outboxMessageId = null)
        where TEnvelope : KafkaDeadLetterEnvelope<TMessage>, new()
    {
        return new TEnvelope
        {
            Payload = payload,
            MessageType = messageType,
            RetryCount = retryCount,
            LastError = error,
            FailedAt = failedAt,
            OccurredAt = occurredAt,
            OutboxMessageId = outboxMessageId
        };
    }
}
