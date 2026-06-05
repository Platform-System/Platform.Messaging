using MassTransit;
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

    public static IServiceCollection AddRabbitMqMessaging(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<IBusRegistrationConfigurator>? configureBus = null)
    {
        services.AddOptions<RabbitMqOptions>()
            .Bind(configuration.GetSection(ConfigurationSections.RabbitMq))
            .Validate(s => !string.IsNullOrWhiteSpace(s.HostName), RabbitMqValidationMessages.HostNameRequired)
            .Validate(s => !string.IsNullOrWhiteSpace(s.UserName), RabbitMqValidationMessages.UserNameRequired)
            .Validate(s => !string.IsNullOrWhiteSpace(s.Password), RabbitMqValidationMessages.PasswordRequired)
            .ValidateOnStart();

        services.AddMassTransit(configurator =>
        {
            configureBus?.Invoke(configurator);

            configurator.UsingRabbitMq((context, busConfigurator) =>
            {
                var options = context.GetRequiredService<Microsoft.Extensions.Options.IOptions<RabbitMqOptions>>().Value;

                busConfigurator.Host(options.HostName, options.VirtualHost, hostConfigurator =>
                {
                    hostConfigurator.Username(options.UserName);
                    hostConfigurator.Password(options.Password);
                });

                busConfigurator.ConfigureEndpoints(context);
            });
        });

        services.AddScoped<IMessagePublisher, RabbitMqMessagePublisher>();

        return services;
    }
}
