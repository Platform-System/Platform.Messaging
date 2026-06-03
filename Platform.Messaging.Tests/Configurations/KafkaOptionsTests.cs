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
    }
}
