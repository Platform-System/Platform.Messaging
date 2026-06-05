using Confluent.Kafka;

namespace Platform.Messaging.Abstractions;

public interface IKafkaConsumerFactory
{
    IConsumer<string, string> Create(string groupId, params string[] topics);
}
