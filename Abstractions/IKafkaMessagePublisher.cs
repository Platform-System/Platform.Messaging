namespace Platform.Messaging.Abstractions;

public interface IKafkaMessagePublisher
{
    Task PublishAsync<TMessage>(string topic, string key, TMessage message, CancellationToken cancellationToken = default);
}
