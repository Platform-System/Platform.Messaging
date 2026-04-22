using MassTransit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Platform.Messaging.Abstractions;
using Platform.Messaging.Configurations;
using Platform.Messaging.Constants;
using Platform.Messaging.Implementations;

namespace Platform.Messaging.DependencyInjection;

public static class DependencyInjection
{
    public static IServiceCollection AddPlatformRabbitMqMessaging(
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
