namespace Platform.Messaging.Configurations;

public interface IReconciliationOptions
{
    bool Enabled { get; }
    int IntervalSeconds { get; }
}
