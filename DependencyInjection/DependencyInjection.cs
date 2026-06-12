using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Platform.Messaging.Abstractions;
using Platform.Messaging.Configurations;
using Platform.Messaging.Constants;
using Platform.Messaging.Helpers;
using Platform.Messaging.Implementations;

namespace Platform.Messaging.DependencyInjection;

public static class DependencyInjection
{
    public static IServiceCollection AddKafkaMessaging(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(ConfigurationSections.Kafka))
            .Validate(s => !string.IsNullOrWhiteSpace(s.BootstrapServers), KafkaValidationMessages.BootstrapServersRequired)
            .Validate(
                s => string.IsNullOrWhiteSpace(s.SecurityProtocol)
                    || Enum.TryParse<SecurityProtocol>(s.SecurityProtocol, true, out _),
                KafkaValidationMessages.SecurityProtocolInvalid)
            .Validate(
                s => string.IsNullOrWhiteSpace(s.SaslMechanism)
                    || Enum.TryParse<SaslMechanism>(s.SaslMechanism, true, out _),
                KafkaValidationMessages.SaslMechanismInvalid)
            .Validate(
                s => string.IsNullOrWhiteSpace(s.ConsumerAutoOffsetReset)
                    || Enum.TryParse<AutoOffsetReset>(s.ConsumerAutoOffsetReset, true, out _),
                KafkaValidationMessages.ConsumerAutoOffsetResetInvalid)
            .Validate(s => s.ProducerMessageSendMaxRetries is null or > 0, KafkaValidationMessages.ProducerMessageSendMaxRetriesInvalid)
            .Validate(s => s.ProducerRetryBackoffMs is null or > 0, KafkaValidationMessages.ProducerRetryBackoffMsInvalid)
            .Validate(s => s.ProducerRetryBackoffMaxMs is null or > 0, KafkaValidationMessages.ProducerRetryBackoffMaxMsInvalid)
            .Validate(s => s.ProducerRequestTimeoutMs is null or > 0, KafkaValidationMessages.ProducerRequestTimeoutMsInvalid)
            .Validate(s => s.ProducerMessageTimeoutMs is null or > 0, KafkaValidationMessages.ProducerMessageTimeoutMsInvalid)
            .Validate(
                s => s.ProducerRetryBackoffMaxMs is null || s.ProducerRetryBackoffMs is null || s.ProducerRetryBackoffMaxMs >= s.ProducerRetryBackoffMs,
                KafkaValidationMessages.ProducerRetryBackoffRangeInvalid)
            .Validate(
                s => s.ProducerMessageTimeoutMs is null || s.ProducerRequestTimeoutMs is null || s.ProducerMessageTimeoutMs >= s.ProducerRequestTimeoutMs,
                KafkaValidationMessages.ProducerMessageTimeoutRangeInvalid)
            .ValidateOnStart();

        services.AddSingleton<IProducer<string, string>>(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaOptions>>().Value;
            var producerConfig = KafkaClientConfigFactory.CreateProducerConfig(options);

            return new ProducerBuilder<string, string>(producerConfig).Build();
        });
        services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
        services.AddSingleton<IKafkaMessagePublisher, KafkaMessagePublisher>();

        return services;
    }
}
