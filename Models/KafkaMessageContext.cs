namespace Platform.Messaging.Models;

public sealed record KafkaMessageContext<TMessage>(
    TMessage Message,
    int RetryCount,
    DateTime? NextAttemptAt,
    string SourceTopic,
    int? OriginalPartition,
    long? OriginalOffset);
