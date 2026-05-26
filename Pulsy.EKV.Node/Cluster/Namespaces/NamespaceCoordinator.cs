using Microsoft.Extensions.Options;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Cluster.Registry;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Diagnostics;
using Pulsy.EKV.Node.Storage;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Cluster.Namespaces;

public sealed class NamespaceCoordinator : IHostedService, IAsyncDisposable
{
    private readonly INamespaceRegistry _registry;
    private readonly DatabasePool _pool;
    private readonly NodeConfig _nodeConfig;
    private readonly ClusterConfig _clusterConfig;
    private readonly EkvMetrics _metrics;
    private readonly ILogger<NamespaceCoordinator> _logger;

    private readonly ILeaseManager? _leaseManager;

    private readonly SemaphoreSlim _namespaceLock = new(1, 1);
    private readonly NamespaceLifecycleTracker _lifecycle = new();
    private Timer? _renewTimer;
    private volatile bool _healthy = true;
    private volatile bool _stopping;

    public NamespaceCoordinator(
        INamespaceRegistry registry,
        DatabasePool pool,
        EkvMetrics metrics,
        IOptions<NodeConfig> nodeConfig,
        IOptions<ClusterConfig> clusterConfig,
        ILogger<NamespaceCoordinator> logger,
        ILeaseManager? leaseManager = null)
    {
        _registry = registry;
        _pool = pool;
        _metrics = metrics;
        _nodeConfig = nodeConfig.Value;
        _clusterConfig = clusterConfig.Value;
        _logger = logger;
        _leaseManager = leaseManager;
    }

    public bool IsHealthy => _healthy;

    private ILeaseManager LeaseManager => _leaseManager
        ?? throw new InvalidOperationException("ILeaseManager is required in cluster mode");

    public async Task StartAsync(CancellationToken ct)
    {
        await _registry.InitAsync(ct);

        if (_clusterConfig.ClusterMode)
        {
            var renewInterval = TimeSpan.FromSeconds(_clusterConfig.LeaseRenewSeconds);
            _renewTimer = new Timer(_ => _ = RenewLeasesAsync(), null, renewInterval, Timeout.InfiniteTimeSpan);
        }

        _logger.LogInformation("NamespaceCoordinator started (node: {NodeId})", _nodeConfig.Id);
    }

    public async Task StopAsync(CancellationToken ct)
    {
        _stopping = true;
        _healthy = false;

        if (_clusterConfig.ClusterMode)
        {
            _renewTimer?.Change(Timeout.Infinite, Timeout.Infinite);

            foreach (var ns in LeaseManager.OwnedNamespaces.ToList())
            {
                await ReleaseNamespaceAsync(ns);
            }
        }

        // Single-node: DatabasePool.StopAsync() handles cleanup
        _logger.LogInformation("NamespaceCoordinator stopped");
    }

    public async Task<StoreHandle?> GetStoreAsync(string namespaceName, CancellationToken ct = default)
    {
        if (_lifecycle.IsReleasing(namespaceName))
        {
            return null;
        }

        var handle = _pool.Acquire(namespaceName);
        if (handle != null)
        {
            if (_lifecycle.IsReleasing(namespaceName))
            {
                handle.Dispose();
                return null;
            }

            if (!_clusterConfig.ClusterMode || LeaseManager.IsOwnedLocally(namespaceName))
            {
                return handle;
            }

            handle.Dispose();
            await _pool.CloseAsync(namespaceName);
        }

        var config = await _registry.GetAsync(namespaceName, ct);
        if (config == null)
        {
            return null;
        }

        await _namespaceLock.WaitAsync(ct);
        try
        {
            var store = await OpenNamespaceUnderLockAsync(namespaceName, config.Backend, ct);
            return store == null ? null : _pool.Acquire(namespaceName);
        }
        finally
        {
            _namespaceLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_renewTimer != null)
        {
            await _renewTimer.DisposeAsync();
        }

        _namespaceLock.Dispose();
    }

    internal async Task<SlateDbStore?> EnsureNamespaceOpenAsync(
        string namespaceName,
        string backend,
        CancellationToken ct = default)
    {
        if (_stopping)
        {
            return null;
        }

        await _namespaceLock.WaitAsync(ct);
        try
        {
            return await OpenNamespaceUnderLockAsync(namespaceName, backend, ct);
        }
        finally
        {
            _namespaceLock.Release();
        }
    }

    internal async Task ReleaseNamespaceAsync(string namespaceName)
    {
        IDisposable releasing;
        await _namespaceLock.WaitAsync();
        try
        {
            releasing = _lifecycle.EnterReleasing(namespaceName);
        }
        finally
        {
            _namespaceLock.Release();
        }

        try
        {
            await _pool.CloseAsync(namespaceName);

            if (_clusterConfig.ClusterMode)
            {
                await LeaseManager.ReleaseAsync(namespaceName);
            }

            _metrics.RecordLeaseReleased();
            _logger.LogInformation("Released namespace {Namespace}", namespaceName);
        }
        finally
        {
            releasing.Dispose();
        }
    }

    internal async Task CloseLocalNamespaceAsync(string namespaceName)
    {
        using var closing = _lifecycle.EnterClosing(namespaceName);
        await _pool.CloseAsync(namespaceName);
    }

    private async Task<SlateDbStore?> OpenNamespaceUnderLockAsync(
        string namespaceName,
        string backend,
        CancellationToken ct)
    {
        if (_stopping || _lifecycle.IsReleasing(namespaceName))
        {
            return null;
        }

        if (!await AcquireLeaseIfNeededAsync(namespaceName, ct))
        {
            return null;
        }

        try
        {
            using var opening = _lifecycle.EnterOpening(namespaceName);
            var store = await _pool.GetOrOpenAsync(namespaceName, backend);
            _logger.LogInformation("Opened namespace {Namespace}", namespaceName);

            return store;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open DB for namespace {Namespace}, releasing", namespaceName);
            await _pool.CloseAsync(namespaceName);

            if (_clusterConfig.ClusterMode)
            {
                await LeaseManager.ReleaseAsync(namespaceName, ct);
            }

            return null;
        }
    }

    private async Task<bool> AcquireLeaseIfNeededAsync(string namespaceName, CancellationToken ct)
    {
        if (!_clusterConfig.ClusterMode || LeaseManager.IsOwnedLocally(namespaceName))
        {
            return true;
        }

        if (!await LeaseManager.TryAcquireAsync(namespaceName, ct))
        {
            _logger.LogDebug("Failed to acquire lease for {Namespace} (already claimed)", namespaceName);
            return false;
        }

        _metrics.RecordLeaseAcquired();
        _logger.LogInformation("Lease acquired for namespace {Namespace}", namespaceName);
        return true;
    }

    private async Task RenewLeasesAsync()
    {
        try
        {
            var openNamespaces = _pool.ListOpenNamespaces()
                .ToDictionary(e => e.Name, e => e.BackendName, StringComparer.Ordinal);

            foreach (var ns in LeaseManager.OwnedNamespaces.ToList())
            {
                try
                {
                    if (_lifecycle.IsClosingOrReleasing(ns))
                    {
                        await RenewLeaseAsync(ns);
                        continue;
                    }

                    if (!await ReconcileRegistryAsync(ns, openNamespaces))
                    {
                        continue;
                    }

                    if (!openNamespaces.ContainsKey(ns) && !_lifecycle.IsOpening(ns))
                    {
                        _metrics.RecordLeaseReleased();
                        _logger.LogInformation("Releasing lease for closed namespace {Namespace}", ns);
                        await LeaseManager.ReleaseAsync(ns);
                        continue;
                    }

                    await RenewLeaseAsync(ns);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error renewing lease for {Namespace}", ns);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during lease renewal cycle");
        }
        finally
        {
            var renewInterval = TimeSpan.FromSeconds(_clusterConfig.LeaseRenewSeconds);
            _renewTimer?.Change(renewInterval, Timeout.InfiniteTimeSpan);
        }
    }

    private async Task<bool> ReconcileRegistryAsync(
        string namespaceName,
        IReadOnlyDictionary<string, string> openNamespaces)
    {
        if (!openNamespaces.TryGetValue(namespaceName, out var backendName))
        {
            return true;
        }

        var config = await _registry.GetAsync(namespaceName);
        if (config == null)
        {
            _logger.LogInformation(
                "Namespace {Namespace} no longer exists in registry, releasing local lease",
                namespaceName);
            await ReleaseNamespaceAsync(namespaceName);
            return false;
        }

        if (!string.Equals(config.Backend, backendName, StringComparison.Ordinal))
        {
            _logger.LogInformation(
                "Namespace {Namespace} backend changed from {OldBackend} to {NewBackend}, releasing local lease",
                namespaceName,
                backendName,
                config.Backend);
            await ReleaseNamespaceAsync(namespaceName);
            return false;
        }

        return true;
    }

    private async Task RenewLeaseAsync(string namespaceName)
    {
        if (await LeaseManager.TryRenewAsync(namespaceName))
        {
            _metrics.RecordLeaseRenewed();
            return;
        }

        _metrics.RecordLeaseLost();
        _logger.LogWarning("Lost lease for {Namespace}, releasing resources", namespaceName);
        await _pool.CloseAsync(namespaceName);
    }
}
