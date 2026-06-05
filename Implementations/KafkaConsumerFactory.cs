using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Platform.Messaging.Abstractions;
using Platform.Messaging.Configurations;
using Platform.Messaging.Helpers;

namespace Platform.Messaging.Implementations;

public sealed class KafkaConsumerFactory : IKafkaConsumerFactory
{
    private readonly KafkaOptions _options;

    public KafkaConsumerFactory(IOptions<KafkaOptions> options)
    {
        _options = options.Value;
    }

    public IConsumer<string, string> Create(string groupId, params string[] topics)
    {
        var consumerConfig = KafkaClientConfigFactory.CreateConsumerConfig(_options, groupId);
        var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        if (topics.Length > 0)
            consumer.Subscribe(topics);

        return consumer;
    }
}
