using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
            [$"{ConfigurationSections.Kafka}:SecurityProtocol"] = "SaslSsl",
            [$"{ConfigurationSections.Kafka}:SaslMechanism"] = "Plain",
            [$"{ConfigurationSections.Kafka}:ConsumerAutoOffsetReset"] = "Earliest",
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
        Assert.Equal("SaslSsl", options.SecurityProtocol);
        Assert.Equal("Plain", options.SaslMechanism);
        Assert.Equal("Earliest", options.ConsumerAutoOffsetReset);
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
    public void AddKafkaMessaging_WithInvalidSecurityProtocol_ThrowsOptionsValidationExceptionOnAccess()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.Kafka}:BootstrapServers"] = "kafka:29092",
            [$"{ConfigurationSections.Kafka}:SecurityProtocol"] = "NotAProtocol"
        });

        services.AddKafkaMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        Action action = () => _ = provider.GetRequiredService<IOptions<KafkaOptions>>().Value;
        var exception = Assert.Throws<OptionsValidationException>(action);

        Assert.Contains(KafkaValidationMessages.SecurityProtocolInvalid, exception.Failures);
    }

    [Fact]
    public void AddKafkaMessaging_WithInvalidConsumerAutoOffsetReset_ThrowsOptionsValidationExceptionOnAccess()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            [$"{ConfigurationSections.Kafka}:BootstrapServers"] = "kafka:29092",
            [$"{ConfigurationSections.Kafka}:ConsumerAutoOffsetReset"] = "Beginning"
        });

        services.AddKafkaMessaging(configuration);
        using var provider = services.BuildServiceProvider();

        Action action = () => _ = provider.GetRequiredService<IOptions<KafkaOptions>>().Value;
        var exception = Assert.Throws<OptionsValidationException>(action);

        Assert.Contains(KafkaValidationMessages.ConsumerAutoOffsetResetInvalid, exception.Failures);
    }

    private static IConfiguration BuildConfiguration(Dictionary<string, string?> values)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();
}
