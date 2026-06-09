using Microsoft.Extensions.Logging.Abstractions;
using Platform.Messaging.Hosting;
using Xunit;

namespace Platform.Messaging.Tests.Hosting;

public sealed class KafkaOutboxDispatcherBaseTests
{
    [Fact]
    public async Task DispatchBatchAsyncCore_WhenPublishFailsBelowMaxRetry_SchedulesRetry()
    {
        var message = new TestClaimedMessage { RetryCount = 0 };
        var dispatcher = new TestOutboxDispatcher(
            [message],
            maxRetryCount: 3,
            publishException: new InvalidOperationException("publish failed"));

        await dispatcher.DispatchOnceAsync();

        Assert.Single(dispatcher.ScheduledRetries);
        Assert.Empty(dispatcher.MarkProcessedMessages);
        Assert.Empty(dispatcher.DeadLetterMessages);
    }

    [Fact]
    public async Task DispatchBatchAsyncCore_WhenDeadLetterSucceedsAtRetryLimit_MarksMessageProcessed()
    {
        var message = new TestClaimedMessage { RetryCount = 2 };
        var dispatcher = new TestOutboxDispatcher(
            [message],
            maxRetryCount: 3,
            publishException: new InvalidOperationException("publish failed"));

        await dispatcher.DispatchOnceAsync();

        Assert.Single(dispatcher.DeadLetterMessages);
        Assert.Single(dispatcher.MarkProcessedMessages);
        Assert.Equal(3, message.RetryCount);
    }

    private sealed class TestOutboxDispatcher : KafkaOutboxDispatcherBase<TestClaimedMessage>
    {
        private readonly IReadOnlyCollection<TestClaimedMessage> _messages;
        private readonly Exception? _publishException;

        public TestOutboxDispatcher(
            IReadOnlyCollection<TestClaimedMessage> messages,
            int maxRetryCount,
            Exception? publishException = null)
            : base(1, maxRetryCount, NullLogger<TestOutboxDispatcher>.Instance)
        {
            _messages = messages;
            _publishException = publishException;
        }

        public List<TestClaimedMessage> MarkProcessedMessages { get; } = [];
        public List<TestClaimedMessage> DeadLetterMessages { get; } = [];
        public List<(TestClaimedMessage Message, int RetryCount, string Error)> ScheduledRetries { get; } = [];

        public Task DispatchOnceAsync()
            => DispatchBatchAsyncCore(CancellationToken.None);

        protected override Task<IReadOnlyCollection<TestClaimedMessage>> ClaimDueMessagesAsync(CancellationToken cancellationToken)
            => Task.FromResult(_messages);

        protected override int GetRetryCount(TestClaimedMessage message) => message.RetryCount;

        protected override void SetRetryCount(TestClaimedMessage message, int retryCount) => message.RetryCount = retryCount;

        protected override Task PublishAsync(TestClaimedMessage message, CancellationToken cancellationToken)
            => _publishException is null ? Task.CompletedTask : Task.FromException(_publishException);

        protected override Task PublishDeadLetterAsync(TestClaimedMessage message, string error, CancellationToken cancellationToken)
        {
            DeadLetterMessages.Add(message);
            return Task.CompletedTask;
        }

        protected override Task MarkProcessedAsync(TestClaimedMessage message, CancellationToken cancellationToken)
        {
            MarkProcessedMessages.Add(message);
            return Task.CompletedTask;
        }

        protected override Task<DateTime> ScheduleRetryAsync(TestClaimedMessage message, int retryCount, string error, CancellationToken cancellationToken)
        {
            ScheduledRetries.Add((message, retryCount, error));
            return Task.FromResult(DateTime.UtcNow.AddMinutes(1));
        }
    }

    private sealed class TestClaimedMessage
    {
        public int RetryCount { get; set; }
    }
}
