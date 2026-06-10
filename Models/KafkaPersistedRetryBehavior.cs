namespace Platform.Messaging.Models;

public sealed class KafkaPersistedRetryBehavior<TMessage>
    where TMessage : class
{
    public required Func<TMessage, bool> IsPayloadValid { get; init; }
    public required Func<TMessage, string?> KeySelector { get; init; }
    public required Func<KafkaRetryEnvelope<TMessage>> CreateInvalidPersistedEnvelope { get; init; }
}
