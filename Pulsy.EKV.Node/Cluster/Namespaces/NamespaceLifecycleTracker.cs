namespace Pulsy.EKV.Node.Cluster.Namespaces;

[Flags]
internal enum NamespaceLifecycleState
{
    None = 0,
    Opening = 1,
    Closing = 2,
    Releasing = 4,
}

internal sealed class NamespaceLifecycleTracker
{
    private readonly Lock _lock = new();
    private readonly Dictionary<string, NamespaceLifecycleState> _states = new(StringComparer.Ordinal);

    public bool IsOpening(string namespaceName) => Has(namespaceName, NamespaceLifecycleState.Opening);

    public bool IsReleasing(string namespaceName) => Has(namespaceName, NamespaceLifecycleState.Releasing);

    public bool IsClosingOrReleasing(string namespaceName)
        => Has(namespaceName, NamespaceLifecycleState.Closing | NamespaceLifecycleState.Releasing);

    public IDisposable EnterOpening(string namespaceName)
        => Enter(namespaceName, NamespaceLifecycleState.Opening);

    public IDisposable EnterClosing(string namespaceName)
        => Enter(namespaceName, NamespaceLifecycleState.Closing);

    public IDisposable EnterReleasing(string namespaceName)
        => Enter(namespaceName, NamespaceLifecycleState.Releasing);

    private bool Has(string namespaceName, NamespaceLifecycleState state)
    {
        lock (_lock)
        {
            return _states.TryGetValue(namespaceName, out var current)
                && (current & state) != NamespaceLifecycleState.None;
        }
    }

    private IDisposable Enter(string namespaceName, NamespaceLifecycleState state)
    {
        lock (_lock)
        {
            _states[namespaceName] = _states.GetValueOrDefault(namespaceName) | state;
        }

        return new Scope(() => Exit(namespaceName, state));
    }

    private void Exit(string namespaceName, NamespaceLifecycleState state)
    {
        lock (_lock)
        {
            if (!_states.TryGetValue(namespaceName, out var current))
            {
                return;
            }

            var next = current & ~state;
            if (next == NamespaceLifecycleState.None)
            {
                _states.Remove(namespaceName);
                return;
            }

            _states[namespaceName] = next;
        }
    }

    private sealed class Scope : IDisposable
    {
        private readonly Action _onDispose;
        private int _disposed;

        public Scope(Action onDispose)
        {
            _onDispose = onDispose;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _onDispose();
            }
        }
    }
}
