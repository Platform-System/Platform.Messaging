using Confluent.Kafka;
using Platform.Messaging.Helpers;
using Xunit;

namespace Platform.Messaging.Tests.Helpers;

public sealed class KafkaRetryPartitionSchedulerTests
{
    [Fact]
    public void TrySchedulePause_WhenNextAttemptIsInFuture_ReturnsTrue()
    {
        var scheduler = new KafkaRetryPartitionScheduler();
        var partition = new TopicPartition("retry-topic", new Partition(1));
        var now = new DateTime(2026, 6, 7, 8, 0, 0, DateTimeKind.Utc);

        var paused = scheduler.TrySchedulePause(partition, now.AddSeconds(10), now);

        Assert.True(paused);
    }

    [Fact]
    public void TrySchedulePause_WhenNextAttemptIsDue_ReturnsFalse()
    {
        var scheduler = new KafkaRetryPartitionScheduler();
        var partition = new TopicPartition("retry-topic", new Partition(1));
        var now = new DateTime(2026, 6, 7, 8, 0, 0, DateTimeKind.Utc);

        var paused = scheduler.TrySchedulePause(partition, now, now);

        Assert.False(paused);
    }

    [Fact]
    public void GetDuePartitions_WhenScheduledPartitionBecomesDue_ReturnsAndRemovesIt()
    {
        var scheduler = new KafkaRetryPartitionScheduler();
        var partition = new TopicPartition("retry-topic", new Partition(1));
        var now = new DateTime(2026, 6, 7, 8, 0, 0, DateTimeKind.Utc);
        scheduler.TrySchedulePause(partition, now.AddSeconds(10), now);

        var duePartitions = scheduler.GetDuePartitions([partition], now.AddSeconds(10));
        var duePartitionsAfterRemoval = scheduler.GetDuePartitions([partition], now.AddSeconds(11));

        Assert.Equal([partition], duePartitions);
        Assert.Empty(duePartitionsAfterRemoval);
    }

    [Fact]
    public void GetDuePartitions_WhenPartitionIsNoLongerAssigned_RemovesStaleState()
    {
        var scheduler = new KafkaRetryPartitionScheduler();
        var partition = new TopicPartition("retry-topic", new Partition(1));
        var now = new DateTime(2026, 6, 7, 8, 0, 0, DateTimeKind.Utc);
        scheduler.TrySchedulePause(partition, now.AddSeconds(10), now);

        var duePartitions = scheduler.GetDuePartitions([], now.AddSeconds(20));
        var duePartitionsAfterCleanup = scheduler.GetDuePartitions([partition], now.AddSeconds(21));

        Assert.Empty(duePartitions);
        Assert.Empty(duePartitionsAfterCleanup);
    }
}
