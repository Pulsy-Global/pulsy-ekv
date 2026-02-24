namespace Pulsy.EKV.Node.Storage.DatabasePool;

public sealed class StoreHandle : IDisposable
{
    private readonly Action _onRelease;
    private int _released;

    internal StoreHandle(SlateDbStore store, Action onRelease)
    {
        Store = store;
        _onRelease = onRelease;
    }

    public SlateDbStore Store { get; }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0)
        {
            _onRelease();
        }
    }
}
