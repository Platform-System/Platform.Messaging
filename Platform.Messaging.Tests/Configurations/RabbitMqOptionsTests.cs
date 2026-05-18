using Platform.Messaging.Configurations;
using Xunit;

namespace Platform.Messaging.Tests.Configurations;

public sealed class RabbitMqOptionsTests
{
    [Fact]
    public void VirtualHost_WhenNotSet_DefaultsToRoot()
    {
        var options = new RabbitMqOptions();

        Assert.Equal("/", options.VirtualHost);
        Assert.Equal(string.Empty, options.HostName);
        Assert.Equal(string.Empty, options.UserName);
        Assert.Equal(string.Empty, options.Password);
    }
}
