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

        ApplyProducerRetryOptions(config, options);
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

    private static void ApplyProducerRetryOptions(ProducerConfig config, KafkaOptions options)
    {
        if (options.ProducerMessageSendMaxRetries is > 0)
            config.MessageSendMaxRetries = options.ProducerMessageSendMaxRetries;

        if (options.ProducerRetryBackoffMs is > 0)
            config.RetryBackoffMs = options.ProducerRetryBackoffMs;

        if (options.ProducerRetryBackoffMaxMs is > 0)
            config.RetryBackoffMaxMs = options.ProducerRetryBackoffMaxMs;

        if (options.ProducerRequestTimeoutMs is > 0)
            config.RequestTimeoutMs = options.ProducerRequestTimeoutMs;

        if (options.ProducerMessageTimeoutMs is > 0)
            config.MessageTimeoutMs = options.ProducerMessageTimeoutMs;
    }
}
