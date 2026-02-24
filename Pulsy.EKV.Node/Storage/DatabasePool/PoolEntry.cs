namespace Pulsy.EKV.Node.Storage.DatabasePool;

internal sealed class PoolEntry
{
    private int _activeOps;

    public PoolEntry(SlateDbStore store, string backendName)
    {
        Store = store;
        BackendName = backendName;
        LastAccess = DateTime.UtcNow;
    }

    public SlateDbStore Store { get; }

    public string BackendName { get; }

    public DateTime LastAccess { get; set; }

    public int ActiveOps => _activeOps;

    public void IncrementOps() => Interlocked.Increment(ref _activeOps);

    public void DecrementOps() => Interlocked.Decrement(ref _activeOps);
}
