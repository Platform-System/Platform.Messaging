namespace Platform.Messaging.Helpers;

public static class RetryDelayCalculator
{
    private const double JitterRatio = 0.2d;

    public static TimeSpan Calculate(int retryCount, int baseDelaySeconds, int maxDelaySeconds, Random? random = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(retryCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(baseDelaySeconds, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDelaySeconds, 1);

        var exponent = Math.Max(0, retryCount - 1);
        var delaySeconds = Math.Min(
            maxDelaySeconds,
            baseDelaySeconds * (int)Math.Pow(2, exponent));

        var maxJitterSeconds = Math.Max(1, (int)Math.Ceiling(delaySeconds * JitterRatio));
        var jitterSeconds = (random ?? Random.Shared).Next(0, maxJitterSeconds + 1);
        var delayedWithJitterSeconds = Math.Min(maxDelaySeconds, delaySeconds + jitterSeconds);

        return TimeSpan.FromSeconds(delayedWithJitterSeconds);
    }
}
