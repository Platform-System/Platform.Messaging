namespace Platform.Messaging.Models;

public class KafkaRetryEnvelope<TMessage>
{
    public TMessage Payload { get; set; } = default!;
    public int RetryCount { get; set; }
    public string SourceTopic { get; set; } = string.Empty;
    public string LastError { get; set; } = string.Empty;
    public DateTime OccurredAt { get; set; }
    public DateTime FailedAt { get; set; }
    public DateTime? NextAttemptAt { get; set; }
    public int? OriginalPartition { get; set; }
    public long? OriginalOffset { get; set; }
}
