using System.Text.Json;
using Platform.Messaging.Abstractions;
using Platform.Messaging.Configurations;
using Platform.Messaging.Models;

namespace Platform.Messaging.Helpers;

public static class KafkaPersistedRetryProcessor
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static async Task<bool> ProcessDueRetryMessageAsync<TMessage, TRetryRecord>(
        Func<CancellationToken, Task<TRetryRecord?>> claimDueRetryAsync,
        Func<TRetryRecord, Guid> getRetryRecordId,
        Func<TRetryRecord, string> getPayload,
        Func<Guid, CancellationToken, Task> deleteRetryAsync,
        Func<Guid, KafkaRetryEnvelope<TMessage>, CancellationToken, Task> updateRetryAsync,
        Func<TMessage, CancellationToken, Task<KafkaMessageProcessResult>> processMessageAsync,
        Func<KafkaMessageContext<TMessage>, string, int, KafkaRetryEnvelope<TMessage>> createRetryEnvelope,
        Func<TMessage, bool> isPayloadValid,
        Func<TMessage, string?> keySelector,
        Func<KafkaRetryEnvelope<TMessage>> createInvalidPersistedEnvelope,
        IKafkaMessagePublisher publisher,
        KafkaConsumerTopicRetryOptions consumerOptions,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        var retryRecord = await claimDueRetryAsync(cancellationToken);
        if (retryRecord is null)
            return false;

        var retryRecordId = getRetryRecordId(retryRecord);
        var retryEnvelope = JsonSerializer.Deserialize<KafkaRetryEnvelope<TMessage>>(getPayload(retryRecord), JsonOptions);

        if (retryEnvelope?.Payload is null || !isPayloadValid(retryEnvelope.Payload))
        {
            var invalidEnvelope = retryEnvelope ?? createInvalidPersistedEnvelope();
            invalidEnvelope.NextAttemptAt = null;

            await publisher.PublishAsync(
                consumerOptions.DeadLetterTopic,
                keySelector(invalidEnvelope.Payload) ?? Guid.NewGuid().ToString("N"),
                invalidEnvelope,
                cancellationToken);

            await deleteRetryAsync(retryRecordId, cancellationToken);
            return true;
        }

        var result = await processMessageAsync(retryEnvelope.Payload, cancellationToken);
        if (result.IsSuccess)
        {
            await deleteRetryAsync(retryRecordId, cancellationToken);
            return true;
        }

        var error = string.Join(", ", result.Errors);
        if (retryEnvelope.RetryCount + 1 < consumerOptions.MaxRetryCount.GetValueOrDefault())
        {
            var updatedEnvelope = createRetryEnvelope(
                new KafkaMessageContext<TMessage>(
                    retryEnvelope.Payload,
                    retryEnvelope.RetryCount,
                    retryEnvelope.NextAttemptAt,
                    retryEnvelope.SourceTopic,
                    retryEnvelope.OriginalPartition,
                    retryEnvelope.OriginalOffset),
                error,
                retryEnvelope.RetryCount + 1);

            await updateRetryAsync(retryRecordId, updatedEnvelope, cancellationToken);
            return true;
        }

        var deadLetterEnvelope = createRetryEnvelope(
            new KafkaMessageContext<TMessage>(
                retryEnvelope.Payload,
                retryEnvelope.RetryCount,
                retryEnvelope.NextAttemptAt,
                retryEnvelope.SourceTopic,
                retryEnvelope.OriginalPartition,
                retryEnvelope.OriginalOffset),
            error,
            retryEnvelope.RetryCount);
        deadLetterEnvelope.NextAttemptAt = null;

        await publisher.PublishAsync(
            consumerOptions.DeadLetterTopic,
            keySelector(retryEnvelope.Payload) ?? Guid.NewGuid().ToString("N"),
            deadLetterEnvelope,
            cancellationToken);

        await deleteRetryAsync(retryRecordId, cancellationToken);
        return true;
    }
}
