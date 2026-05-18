using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Platform.Messaging.Abstractions;
using Platform.Messaging.Configurations;
using Platform.Messaging.Constants;
using Platform.Messaging.DependencyInjection;
using Platform.Messaging.Implementations;
using Xunit;

namespace Platform.Messaging.Tests.DependencyInjection;

public sealed class DependencyInjectionTests
{
    [Fact]
    public void AddPlatformRabbitMqMessaging_WithValidConfig_BindsOptionsAndRegistersPublisher()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.RabbitMq}:HostName"] = "localhost",
            [$"{ConfigurationSections.RabbitMq}:UserName"] = "guest",
            [$"{ConfigurationSections.RabbitMq}:Password"] = "guest",
            [$"{ConfigurationSections.RabbitMq}:VirtualHost"] = "/platform"
        });

        services.AddPlatformRabbitMqMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
        Assert.Equal("localhost", options.HostName);
        Assert.Equal("guest", options.UserName);
        Assert.Equal("guest", options.Password);
        Assert.Equal("/platform", options.VirtualHost);

        var publisherDescriptor = services.SingleOrDefault(x => x.ServiceType == typeof(IMessagePublisher));
        Assert.NotNull(publisherDescriptor);
        Assert.Equal(typeof(RabbitMqMessagePublisher), publisherDescriptor!.ImplementationType);
    }

    [Fact]
    public void AddPlatformRabbitMqMessaging_WithMissingHostName_ThrowsOptionsValidationExceptionOnAccess()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.RabbitMq}:UserName"] = "guest",
            [$"{ConfigurationSections.RabbitMq}:Password"] = "guest"
        });

        services.AddPlatformRabbitMqMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        Action action = () => _ = provider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
        var exception = Assert.Throws<OptionsValidationException>(action);

        Assert.Contains(RabbitMqValidationMessages.HostNameRequired, exception.Failures);
    }

    [Fact]
    public void AddPlatformRabbitMqMessaging_WithMissingPassword_ThrowsOptionsValidationExceptionOnAccess()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.RabbitMq}:HostName"] = "localhost",
            [$"{ConfigurationSections.RabbitMq}:UserName"] = "guest"
        });

        services.AddPlatformRabbitMqMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        Action action = () => _ = provider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
        var exception = Assert.Throws<OptionsValidationException>(action);

        Assert.Contains(RabbitMqValidationMessages.PasswordRequired, exception.Failures);
    }

    private static IConfiguration BuildConfiguration(Dictionary<string, string?> values)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();
}
