namespace Platform.Messaging.Constants;

public static class KafkaValidationMessages
{
    public const string BootstrapServersRequired = "Kafka BootstrapServers is required";
    public const string SecurityProtocolInvalid = "Kafka SecurityProtocol must be a valid Confluent.Kafka.SecurityProtocol value when provided";
    public const string SaslMechanismInvalid = "Kafka SaslMechanism must be a valid Confluent.Kafka.SaslMechanism value when provided";
    public const string ConsumerAutoOffsetResetInvalid = "Kafka ConsumerAutoOffsetReset must be a valid Confluent.Kafka.AutoOffsetReset value when provided";
    public const string ProducerMessageSendMaxRetriesInvalid = "Kafka ProducerMessageSendMaxRetries must be greater than zero when provided";
    public const string ProducerRetryBackoffMsInvalid = "Kafka ProducerRetryBackoffMs must be greater than zero when provided";
    public const string ProducerRetryBackoffMaxMsInvalid = "Kafka ProducerRetryBackoffMaxMs must be greater than zero when provided";
    public const string ProducerRequestTimeoutMsInvalid = "Kafka ProducerRequestTimeoutMs must be greater than zero when provided";
    public const string ProducerMessageTimeoutMsInvalid = "Kafka ProducerMessageTimeoutMs must be greater than zero when provided";
    public const string ProducerRetryBackoffRangeInvalid = "Kafka ProducerRetryBackoffMaxMs must be greater than or equal to ProducerRetryBackoffMs";
    public const string ProducerMessageTimeoutRangeInvalid = "Kafka ProducerMessageTimeoutMs must be greater than or equal to ProducerRequestTimeoutMs";
}
