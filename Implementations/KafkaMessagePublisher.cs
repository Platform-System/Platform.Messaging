using System.Text.Json;
using Confluent.Kafka;
using Platform.Messaging.Abstractions;

namespace Platform.Messaging.Implementations;

public sealed class KafkaMessagePublisher : IKafkaMessagePublisher
{
    private readonly IProducer<string, string> _producer;

    public KafkaMessagePublisher(IProducer<string, string> producer)
    {
        _producer = producer;
    }

    public Task PublishAsync<TMessage>(string topic, string key, TMessage message, CancellationToken cancellationToken = default)
    {
        var payload = JsonSerializer.Serialize(message) ?? string.Empty;

        return _producer.ProduceAsync(
            topic,
            new Message<string, string> { Key = key, Value = payload },
            cancellationToken);
    }
}
