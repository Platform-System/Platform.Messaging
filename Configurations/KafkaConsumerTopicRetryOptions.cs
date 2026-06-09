namespace Platform.Messaging.Configurations;

public class KafkaConsumerTopicRetryOptions : KafkaTopicRetryOptions
{
    public string GroupId { get; set; } = string.Empty;
    public int? BaseRetryDelaySeconds { get; set; }
    public int? MaxRetryDelaySeconds { get; set; }
}
