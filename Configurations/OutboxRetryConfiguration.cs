namespace Platform.Messaging.Configurations;

public sealed class OutboxRetryConfiguration
{
    public static OutboxRetryConfiguration Create(int maxRetryCount) => new() { MaxRetryCount = maxRetryCount };

    public int MaxRetryCount { get; init; }
}
