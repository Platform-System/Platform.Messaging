namespace Platform.Messaging.Models;

public sealed class KafkaMessageProcessResult
{
    public static KafkaMessageProcessResult Success() => new(true, Array.Empty<string>());

    public static KafkaMessageProcessResult Failure(IEnumerable<string> errors)
        => new(false, errors.ToArray());

    public KafkaMessageProcessResult(bool isSuccess, IReadOnlyCollection<string> errors)
    {
        IsSuccess = isSuccess;
        Errors = errors;
    }

    public bool IsSuccess { get; }
    public IReadOnlyCollection<string> Errors { get; }
}
