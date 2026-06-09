namespace Platform.Messaging.Configurations;

public class KafkaTopicRetryOptions
{
    public string Topic { get; set; } = string.Empty;
    public string DeadLetterTopic { get; set; } = string.Empty;
    public int? MaxRetryCount { get; set; }
}
