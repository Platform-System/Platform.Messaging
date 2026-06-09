using Confluent.Kafka;

namespace Platform.Messaging.Helpers;

public sealed class KafkaRetryPartitionScheduler
{
    private readonly Dictionary<TopicPartition, DateTime> _pausedPartitions = [];

    public bool TrySchedulePause(TopicPartition partition, DateTime? nextAttemptAt, DateTime now)
    {
        if (nextAttemptAt is not { } dueAt || dueAt <= now)
            return false;

        _pausedPartitions[partition] = dueAt;
        return true;
    }

    public IReadOnlyList<TopicPartition> GetDuePartitions(IEnumerable<TopicPartition> assignedPartitions, DateTime now)
    {
        if (_pausedPartitions.Count == 0)
            return [];

        var assignedSet = assignedPartitions.ToHashSet();
        var unassignedPartitions = _pausedPartitions.Keys
            .Where(partition => !assignedSet.Contains(partition))
            .ToList();

        foreach (var partition in unassignedPartitions)
            _pausedPartitions.Remove(partition);

        var duePartitions = _pausedPartitions
            .Where(x => x.Value <= now && assignedSet.Contains(x.Key))
            .Select(x => x.Key)
            .ToList();

        foreach (var partition in duePartitions)
            _pausedPartitions.Remove(partition);

        return duePartitions;
    }
}
