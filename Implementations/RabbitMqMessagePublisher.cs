using MassTransit;
using Platform.Messaging.Abstractions;

namespace Platform.Messaging.Implementations;

public sealed class RabbitMqMessagePublisher : IMessagePublisher
{
    private readonly IPublishEndpoint _publishEndpoint;

    public RabbitMqMessagePublisher(IPublishEndpoint publishEndpoint)
    {
        _publishEndpoint = publishEndpoint;
    }

    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
    {
        await _publishEndpoint.Publish(message!, cancellationToken);
    }
}
