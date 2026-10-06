namespace DotnetJobKit.IntegrationTests.Helpers;

public sealed class FakeTimeProvider : TimeProvider
{
    private DateTimeOffset _utcNow;

    public FakeTimeProvider(DateTimeOffset? start = null)
    {
        _utcNow = start ?? DateTimeOffset.UtcNow;
    }

    public void SetUtcNow(DateTimeOffset value) => _utcNow = value;

    public void Advance(TimeSpan delta) => _utcNow += delta;

    public override DateTimeOffset GetUtcNow() => _utcNow;
}
