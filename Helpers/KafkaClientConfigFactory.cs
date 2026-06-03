using Confluent.Kafka;
using Platform.Messaging.Configurations;

namespace Platform.Messaging.Helpers;

public static class KafkaClientConfigFactory
{
    public static ProducerConfig CreateProducerConfig(KafkaOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            EnableIdempotence = true
        };

        ApplySecurityOptions(config, options);
        return config;
    }

    public static ConsumerConfig CreateConsumerConfig(KafkaOptions options, string groupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        ApplySecurityOptions(config, options);
        return config;
    }

    private static void ApplySecurityOptions(ClientConfig config, KafkaOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.SecurityProtocol)
            && Enum.TryParse<SecurityProtocol>(options.SecurityProtocol, true, out var securityProtocol))
        {
            config.SecurityProtocol = securityProtocol;
        }

        if (!string.IsNullOrWhiteSpace(options.SaslMechanism)
            && Enum.TryParse<SaslMechanism>(options.SaslMechanism, true, out var saslMechanism))
        {
            config.SaslMechanism = saslMechanism;
        }

        if (!string.IsNullOrWhiteSpace(options.SaslUsername))
            config.SaslUsername = options.SaslUsername;

        if (!string.IsNullOrWhiteSpace(options.SaslPassword))
            config.SaslPassword = options.SaslPassword;
    }
}
