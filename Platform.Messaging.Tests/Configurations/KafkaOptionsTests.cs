using Platform.Messaging.Configurations;
using Xunit;

namespace Platform.Messaging.Tests.Configurations;

public sealed class KafkaOptionsTests
{
    [Fact]
    public void Defaults_WhenNotSet_AreEmptyStrings()
    {
        var options = new KafkaOptions();

        Assert.Equal(string.Empty, options.BootstrapServers);
        Assert.Equal(string.Empty, options.SecurityProtocol);
        Assert.Equal(string.Empty, options.SaslMechanism);
        Assert.Equal(string.Empty, options.SaslUsername);
        Assert.Equal(string.Empty, options.SaslPassword);
        Assert.Equal(string.Empty, options.ConsumerAutoOffsetReset);
        Assert.Null(options.ProducerMessageSendMaxRetries);
        Assert.Null(options.ProducerRetryBackoffMs);
        Assert.Null(options.ProducerRetryBackoffMaxMs);
        Assert.Null(options.ProducerRequestTimeoutMs);
        Assert.Null(options.ProducerMessageTimeoutMs);
    }
}
