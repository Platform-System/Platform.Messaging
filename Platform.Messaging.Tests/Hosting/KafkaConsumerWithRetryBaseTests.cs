using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Platform.Messaging.Abstractions;
using Platform.Messaging.Hosting;
using Platform.Messaging.Models;
using Xunit;

namespace Platform.Messaging.Tests.Hosting;

public sealed class KafkaConsumerWithRetryBaseTests
{
    [Fact]
    public async Task ExecuteAsync_WhenRetryMessageWasProcessed_StillConsumesNewKafkaMessage()
    {
        var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var consumer = new StubConsumer(
            new ConsumeResult<string, string>
            {
                Topic = "identity.user-synced",
                Partition = new Partition(0),
                Offset = new Offset(10),
                Message = new Message<string, string>
                {
                    Key = "user-1",
                    Value = "payload-1"
                }
            });

        var service = new TestKafkaConsumerService(
            new StubKafkaConsumerFactory(consumer),
            cancellationTokenSource,
            processDueRetryResult: true);

        await service.RunUntilCancelledAsync(cancellationTokenSource.Token);

        Assert.True(service.ProcessDueRetryCalled);
        Assert.Single(service.ProcessedMessages);
        Assert.Equal("payload-1", service.ProcessedMessages[0]);
        Assert.Single(consumer.CommittedResults);
    }

    private sealed class TestKafkaConsumerService : KafkaConsumerWithRetryBase<string>
    {
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly bool _processDueRetryResult;

        public TestKafkaConsumerService(
            IKafkaConsumerFactory consumerFactory,
            CancellationTokenSource cancellationTokenSource,
            bool processDueRetryResult)
            : base(
                "identity.user-synced",
                "wallet-group",
                maxRetryCount: 3,
                consumerFactory,
                new StubKafkaMessagePublisher(),
                NullLogger<TestKafkaConsumerService>.Instance)
        {
            _cancellationTokenSource = cancellationTokenSource;
            _processDueRetryResult = processDueRetryResult;
        }

        public bool ProcessDueRetryCalled { get; private set; }
        public List<string> ProcessedMessages { get; } = [];

        public Task RunUntilCancelledAsync(CancellationToken cancellationToken)
            => ExecuteAsync(cancellationToken);

        protected override KafkaMessageContext<string>? DeserializeMessage(ConsumeResult<string, string> consumeResult)
            => new(
                consumeResult.Message.Value,
                0,
                null,
                consumeResult.Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value);

        protected override bool IsMessageValid(string message) => !string.IsNullOrWhiteSpace(message);

        protected override KafkaRetryEnvelope<string> CreateInvalidMessageEnvelope(ConsumeResult<string, string> consumeResult)
            => new()
            {
                Payload = string.Empty,
                LastError = "invalid"
            };

        protected override KafkaRetryEnvelope<string> CreateRetryEnvelope(KafkaMessageContext<string> context, string error, int retryCount)
            => new()
            {
                Payload = context.Message,
                RetryCount = retryCount,
                LastError = error
            };

        protected override Task<KafkaMessageProcessResult> ProcessMessageAsync(string message, CancellationToken cancellationToken)
        {
            ProcessedMessages.Add(message);
            _cancellationTokenSource.Cancel();
            return Task.FromResult(KafkaMessageProcessResult.Success());
        }

        protected override Task StoreRetryAsync(KafkaMessageContext<string> context, string error, int retryCount, CancellationToken cancellationToken)
            => Task.CompletedTask;

        protected override Task<bool> ProcessDueRetryMessageAsync(CancellationToken cancellationToken)
        {
            ProcessDueRetryCalled = true;
            return Task.FromResult(_processDueRetryResult);
        }

        protected override Task PublishDeadLetterAsync(
            ConsumeResult<string, string>? consumeResult,
            KafkaRetryEnvelope<string> envelope,
            CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class StubKafkaConsumerFactory : IKafkaConsumerFactory
    {
        private readonly IConsumer<string, string> _consumer;

        public StubKafkaConsumerFactory(IConsumer<string, string> consumer)
        {
            _consumer = consumer;
        }

        public IConsumer<string, string> Create(string groupId, params string[] topics) => _consumer;
    }

    private sealed class StubKafkaMessagePublisher : IKafkaMessagePublisher
    {
        public Task PublishAsync<TMessage>(string topic, string key, TMessage message, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    private sealed class StubConsumer : IConsumer<string, string>
    {
        private readonly Queue<ConsumeResult<string, string>?> _consumeResults;

        public StubConsumer(params ConsumeResult<string, string>?[] consumeResults)
        {
            _consumeResults = new Queue<ConsumeResult<string, string>?>(consumeResults);
        }

        public List<ConsumeResult<string, string>> CommittedResults { get; } = [];
        public List<string> SubscribedTopics { get; } = [];

        public string MemberId => string.Empty;
        public List<TopicPartition> Assignment => [];
        public List<string> Subscription => SubscribedTopics;
        public IConsumerGroupMetadata ConsumerGroupMetadata => throw new NotSupportedException();
        public Handle Handle => throw new NotSupportedException();
        public string Name => "stub-consumer";
        public int AddBrokers(string brokers) => 0;
        public void Assign(TopicPartition partition) => throw new NotSupportedException();
        public void Assign(TopicPartitionOffset partition) => throw new NotSupportedException();
        public void Assign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();
        public void Assign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();
        public void Close() { }
        public List<TopicPartitionOffset> Commit() => [];
        public void Commit(ConsumeResult<string, string> result) => CommittedResults.Add(result);
        public void Commit(IEnumerable<TopicPartitionOffset> offsets) { }
        public ConsumeResult<string, string>? Consume(int millisecondsTimeout) => Consume(TimeSpan.FromMilliseconds(millisecondsTimeout));
        public ConsumeResult<string, string>? Consume(CancellationToken cancellationToken) => Consume(TimeSpan.Zero);
        public ConsumeResult<string, string>? Consume(TimeSpan timeout)
            => _consumeResults.Count > 0 ? _consumeResults.Dequeue() : null;
        public void Dispose() { }
        public Metadata GetMetadata(string topic, TimeSpan timeout) => throw new NotSupportedException();
        public Metadata GetMetadata(TimeSpan timeout) => throw new NotSupportedException();
        public IConsumerGroupMetadata GetConsumerGroupMetadata() => throw new NotSupportedException();
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => throw new NotSupportedException();
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => throw new NotSupportedException();
        public List<TopicPartitionOffset> Committed(TimeSpan timeout) => throw new NotSupportedException();
        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => throw new NotSupportedException();
        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout) => throw new NotSupportedException();
        public int Poll(TimeSpan timeout) => throw new NotSupportedException();
        public void Pause(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();
        public void Resume(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();
        public void Seek(TopicPartitionOffset tpo) => throw new NotSupportedException();
        public void StoreOffset(ConsumeResult<string, string> result) => throw new NotSupportedException();
        public void StoreOffset(TopicPartitionOffset offset) => throw new NotSupportedException();
        public Offset Position(TopicPartition partition) => throw new NotSupportedException();
        public List<TopicPartitionOffset> Position(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();
        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();
        public void IncrementalAssign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();
        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();
        public void SetSaslCredentials(string username, string password) => throw new NotSupportedException();
        public void Subscribe(string topic) => SubscribedTopics.Add(topic);
        public void Subscribe(IEnumerable<string> topics) => SubscribedTopics.AddRange(topics);
        public void Unassign() => throw new NotSupportedException();
        public void Unsubscribe() => throw new NotSupportedException();
        public event Action<IConsumer<string, string>, List<TopicPartition>>? OnPartitionsAssigned { add { } remove { } }
        public event Action<IConsumer<string, string>, List<TopicPartitionOffset>>? OnPartitionsRevoked { add { } remove { } }
        public event Action<IConsumer<string, string>, CommittedOffsets>? OnOffsetsCommitted { add { } remove { } }
    }
}
