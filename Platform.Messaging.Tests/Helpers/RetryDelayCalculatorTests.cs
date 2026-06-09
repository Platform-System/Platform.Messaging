using Platform.Messaging.Helpers;
using Xunit;

namespace Platform.Messaging.Tests.Helpers;

public sealed class RetryDelayCalculatorTests
{
    [Fact]
    public void Calculate_WhenFirstRetry_AddsJitterWithinExpectedRange()
    {
        var random = new Random(123);
        var delay = RetryDelayCalculator.Calculate(1, 5, 60, random);

        Assert.InRange(delay, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(6));
    }

    [Fact]
    public void Calculate_WhenRetryCountGrows_CapsAtMaxDelay()
    {
        var random = new Random(123);
        var delay = RetryDelayCalculator.Calculate(10, 5, 60, random);

        Assert.Equal(TimeSpan.FromSeconds(60), delay);
    }

    [Fact]
    public void Calculate_WhenCalledMultipleTimes_CanSpreadRetries()
    {
        var delays = Enumerable.Range(0, 20)
            .Select(seed => RetryDelayCalculator.Calculate(2, 5, 60, new Random(seed)).TotalSeconds)
            .Distinct()
            .OrderBy(x => x)
            .ToArray();

        Assert.NotEmpty(delays);
        Assert.All(delays, delay => Assert.InRange(delay, 10, 12));
        Assert.True(delays.Length > 1);
    }
}
