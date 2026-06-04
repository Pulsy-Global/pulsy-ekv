using Pulsy.EKV.Node.Configuration.Backends;

namespace Pulsy.EKV.Node.Storage.DatabasePool;

internal sealed class PoolEntry
{
    private readonly object _idleLock = new();
    private TaskCompletionSource _idle = NewIdleCompletion();
    private int _activeOps;

    public PoolEntry(
        SlateDbStore store,
        string backendName,
        BackendType backendType,
        string? diskCacheRoot)
    {
        Store = store;
        BackendName = backendName;
        BackendType = backendType;
        DiskCacheRoot = diskCacheRoot;
        LastAccess = DateTime.UtcNow;
    }

    public SlateDbStore Store { get; }

    public string BackendName { get; }

    public BackendType BackendType { get; }

    public string? DiskCacheRoot { get; }

    public DateTime LastAccess { get; set; }

    public int ActiveOps
    {
        get
        {
            lock (_idleLock)
            {
                return _activeOps;
            }
        }
    }

    public void IncrementOps()
    {
        lock (_idleLock)
        {
            _activeOps++;
            if (_activeOps == 1)
            {
                _idle = NewIdleCompletion();
            }
        }
    }

    public void DecrementOps()
    {
        TaskCompletionSource? idle = null;
        lock (_idleLock)
        {
            _activeOps--;
            if (_activeOps == 0)
            {
                idle = _idle;
            }
        }

        idle?.TrySetResult();
    }

    public Task WaitForIdleAsync()
    {
        lock (_idleLock)
        {
            return _activeOps == 0 ? Task.CompletedTask : _idle.Task;
        }
    }

    private static TaskCompletionSource NewIdleCompletion()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
