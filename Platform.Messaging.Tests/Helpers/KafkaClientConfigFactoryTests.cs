using Platform.Messaging.Configurations;
using Platform.Messaging.Helpers;
using Xunit;

namespace Platform.Messaging.Tests.Helpers;

public sealed class KafkaClientConfigFactoryTests
{
    [Fact]
    public void CreateProducerConfig_WhenProducerRetryOptionsAreProvided_AppliesThem()
    {
        var options = new KafkaOptions
        {
            BootstrapServers = "kafka:29092",
            ProducerMessageSendMaxRetries = 5,
            ProducerRetryBackoffMs = 250,
            ProducerRetryBackoffMaxMs = 3000,
            ProducerRequestTimeoutMs = 30000,
            ProducerMessageTimeoutMs = 120000
        };

        var config = KafkaClientConfigFactory.CreateProducerConfig(options);

        Assert.Equal(5, config.MessageSendMaxRetries);
        Assert.Equal(250, config.RetryBackoffMs);
        Assert.Equal(3000, config.RetryBackoffMaxMs);
        Assert.Equal(30000, config.RequestTimeoutMs);
        Assert.Equal(120000, config.MessageTimeoutMs);
    }
}
