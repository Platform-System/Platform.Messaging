namespace Platform.Messaging.Helpers;

public sealed class KafkaOutboxMessageTypeRegistry<TClaimedMessage>
{
    private readonly Dictionary<string, Func<TClaimedMessage, CancellationToken, Task>> _publishHandlers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Func<TClaimedMessage, string, CancellationToken, Task>> _deadLetterHandlers = new(StringComparer.Ordinal);

    public KafkaOutboxMessageTypeRegistry<TClaimedMessage> Register(
        string messageType,
        Func<TClaimedMessage, CancellationToken, Task> publishAsync,
        Func<TClaimedMessage, string, CancellationToken, Task> publishDeadLetterAsync)
    {
        _publishHandlers[messageType] = publishAsync;
        _deadLetterHandlers[messageType] = publishDeadLetterAsync;
        return this;
    }

    public Task PublishAsync(string messageType, TClaimedMessage message, CancellationToken cancellationToken)
    {
        if (!_publishHandlers.TryGetValue(messageType, out var handler))
            throw new InvalidOperationException($"Unsupported outbox message type '{messageType}'.");

        return handler(message, cancellationToken);
    }

    public Task PublishDeadLetterAsync(string messageType, TClaimedMessage message, string error, CancellationToken cancellationToken)
    {
        if (!_deadLetterHandlers.TryGetValue(messageType, out var handler))
            throw new InvalidOperationException($"Unsupported outbox message type '{messageType}'.");

        return handler(message, error, cancellationToken);
    }
}
