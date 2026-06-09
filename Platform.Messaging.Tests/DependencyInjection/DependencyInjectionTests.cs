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
    public void AddKafkaMessaging_WithValidConfig_BindsOptionsAndRegistersKafkaServices()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.Kafka}:BootstrapServers"] = "kafka:29092",
            [$"{ConfigurationSections.Kafka}:ProducerMessageSendMaxRetries"] = "5",
            [$"{ConfigurationSections.Kafka}:ProducerRetryBackoffMs"] = "250",
            [$"{ConfigurationSections.Kafka}:ProducerRetryBackoffMaxMs"] = "3000",
            [$"{ConfigurationSections.Kafka}:ProducerRequestTimeoutMs"] = "30000",
            [$"{ConfigurationSections.Kafka}:ProducerMessageTimeoutMs"] = "120000"
        });

        services.AddKafkaMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<KafkaOptions>>().Value;
        Assert.Equal("kafka:29092", options.BootstrapServers);
        Assert.Equal(5, options.ProducerMessageSendMaxRetries);
        Assert.Equal(250, options.ProducerRetryBackoffMs);
        Assert.Equal(3000, options.ProducerRetryBackoffMaxMs);
        Assert.Equal(30000, options.ProducerRequestTimeoutMs);
        Assert.Equal(120000, options.ProducerMessageTimeoutMs);

        var publisherDescriptor = services.SingleOrDefault(x => x.ServiceType == typeof(IKafkaMessagePublisher));
        Assert.NotNull(publisherDescriptor);
        Assert.Equal(typeof(KafkaMessagePublisher), publisherDescriptor!.ImplementationType);

        var consumerFactoryDescriptor = services.SingleOrDefault(x => x.ServiceType == typeof(IKafkaConsumerFactory));
        Assert.NotNull(consumerFactoryDescriptor);
        Assert.Equal(typeof(KafkaConsumerFactory), consumerFactoryDescriptor!.ImplementationType);
    }

    [Fact]
    public void AddKafkaMessaging_WithMissingBootstrapServers_ThrowsOptionsValidationExceptionOnAccess()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>());

        services.AddKafkaMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        Action action = () => _ = provider.GetRequiredService<IOptions<KafkaOptions>>().Value;
        var exception = Assert.Throws<OptionsValidationException>(action);

        Assert.Contains(KafkaValidationMessages.BootstrapServersRequired, exception.Failures);
    }

    [Fact]
    public void AddKafkaMessaging_WithInvalidProducerRetryRange_ThrowsOptionsValidationExceptionOnAccess()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.Kafka}:BootstrapServers"] = "kafka:29092",
            [$"{ConfigurationSections.Kafka}:ProducerRetryBackoffMs"] = "3000",
            [$"{ConfigurationSections.Kafka}:ProducerRetryBackoffMaxMs"] = "250"
        });

        services.AddKafkaMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        Action action = () => _ = provider.GetRequiredService<IOptions<KafkaOptions>>().Value;
        var exception = Assert.Throws<OptionsValidationException>(action);

        Assert.Contains(KafkaValidationMessages.ProducerRetryBackoffRangeInvalid, exception.Failures);
    }

    [Fact]
    public void AddRabbitMqMessaging_WithValidConfig_BindsOptionsAndRegistersPublisher()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.RabbitMq}:HostName"] = "localhost",
            [$"{ConfigurationSections.RabbitMq}:UserName"] = "guest",
            [$"{ConfigurationSections.RabbitMq}:Password"] = "guest",
            [$"{ConfigurationSections.RabbitMq}:VirtualHost"] = "/platform"
        });

        services.AddRabbitMqMessaging(configuration);
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
    public void AddRabbitMqMessaging_WithMissingHostName_ThrowsOptionsValidationExceptionOnAccess()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.RabbitMq}:UserName"] = "guest",
            [$"{ConfigurationSections.RabbitMq}:Password"] = "guest"
        });

        services.AddRabbitMqMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        Action action = () => _ = provider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
        var exception = Assert.Throws<OptionsValidationException>(action);

        Assert.Contains(RabbitMqValidationMessages.HostNameRequired, exception.Failures);
    }

    [Fact]
    public void AddRabbitMqMessaging_WithMissingPassword_ThrowsOptionsValidationExceptionOnAccess()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.RabbitMq}:HostName"] = "localhost",
            [$"{ConfigurationSections.RabbitMq}:UserName"] = "guest"
        });

        services.AddRabbitMqMessaging(configuration);
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
