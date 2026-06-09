namespace Platform.Messaging.Models;

public class KafkaDeadLetterEnvelope<TMessage>
{
    public TMessage Payload { get; set; } = default!;
    public string MessageType { get; set; } = string.Empty;
    public int RetryCount { get; set; }
    public string LastError { get; set; } = string.Empty;
    public DateTime FailedAt { get; set; }
    public DateTime OccurredAt { get; set; }
    public Guid? OutboxMessageId { get; set; }
}
