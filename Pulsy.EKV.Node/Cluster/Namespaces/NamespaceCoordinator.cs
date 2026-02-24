using System.Text.Json;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Cluster.Messages;
using Pulsy.EKV.Node.Cluster.Placement;
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

    // Cluster-only dependencies (null in single-node mode, accessed via throwing properties)
    private readonly ILeaseManager? _leaseManager;
    private readonly IPlacementStrategy? _placement;
    private readonly INatsConnection? _nats;

    private readonly SemaphoreSlim _claimLock = new(1, 1);
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
        ILeaseManager? leaseManager = null,
        IPlacementStrategy? placement = null,
        INatsConnection? nats = null)
    {
        _registry = registry;
        _pool = pool;
        _metrics = metrics;
        _nodeConfig = nodeConfig.Value;
        _clusterConfig = clusterConfig.Value;
        _logger = logger;
        _leaseManager = leaseManager;
        _placement = placement;
        _nats = nats;
    }

    public bool IsHealthy => _healthy;

    private ILeaseManager LeaseManager => _leaseManager
        ?? throw new InvalidOperationException("ILeaseManager is required in cluster mode");

    private IPlacementStrategy Placement => _placement
        ?? throw new InvalidOperationException("IPlacementStrategy is required in cluster mode");

    private INatsConnection Nats => _nats
        ?? throw new InvalidOperationException("INatsConnection is required in cluster mode");

    public async Task StartAsync(CancellationToken ct)
    {
        await _registry.InitAsync(ct);

        if (_clusterConfig.ClusterMode)
        {
            await Placement.InitAsync(ct);
            await ClaimUnclaimedNamespacesAsync(ct);

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

            var drained = await TryDrainViaLeaderAsync(ct);
            if (!drained)
            {
                foreach (var ns in LeaseManager.OwnedNamespaces.ToList())
                {
                    await ReleaseNamespaceAsync(ns);
                }
            }
            else
            {
                foreach (var ns in LeaseManager.OwnedNamespaces.ToList())
                {
                    await _pool.CloseAsync(ns);
                }
            }
        }

        // Single-node: DatabasePool.StopAsync() handles cleanup
        _logger.LogInformation("NamespaceCoordinator stopped");
    }

    public async Task<StoreHandle?> GetStoreAsync(string namespaceName, CancellationToken ct = default)
    {
        var handle = _pool.Acquire(namespaceName);
        if (handle != null)
        {
            return handle;
        }

        if (_clusterConfig.ClusterMode)
        {
            if (!LeaseManager.IsOwnedLocally(namespaceName))
            {
                return null;
            }
        }

        var config = await _registry.GetAsync(namespaceName, ct);
        if (config == null)
        {
            return null;
        }

        await _pool.GetOrOpenAsync(namespaceName, config.Backend);
        return _pool.Acquire(namespaceName);
    }

    internal async Task<SlateDbStore?> TryClaimAsync(string namespaceName, CancellationToken ct = default)
    {
        if (_stopping)
        {
            return null;
        }

        if (!_clusterConfig.ClusterMode)
        {
            var config = await _registry.GetAsync(namespaceName, ct);
            if (config == null)
            {
                return null;
            }

            var store = await _pool.GetOrOpenAsync(namespaceName, config.Backend);
            _logger.LogInformation("Claimed namespace {Namespace}", namespaceName);
            return store;
        }

        string backend;

        await _claimLock.WaitAsync(ct);
        try
        {
            if (LeaseManager.IsOwnedLocally(namespaceName))
            {
                var existingConfig = await _registry.GetAsync(namespaceName, ct);
                if (existingConfig == null)
                {
                    return null;
                }

                backend = existingConfig.Backend;
            }
            else
            {
                var targetNode = await Placement.SelectNodeAsync(ct);
                if (targetNode != _nodeConfig.Id)
                {
                    _logger.LogDebug("Placement selected {Node} for {Namespace}, not us", targetNode, namespaceName);
                    return null;
                }

                var config = await _registry.GetAsync(namespaceName, ct);
                if (config == null)
                {
                    _logger.LogWarning("Namespace {Namespace} not found in registry", namespaceName);
                    return null;
                }

                if (!await LeaseManager.TryAcquireAsync(namespaceName, ct))
                {
                    _logger.LogDebug("Failed to acquire lease for {Namespace} (already claimed)", namespaceName);
                    return null;
                }

                backend = config.Backend;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to claim namespace {Namespace}", namespaceName);
            return null;
        }
        finally
        {
            _claimLock.Release();
        }

        try
        {
            var store = await _pool.GetOrOpenAsync(namespaceName, backend);

            _metrics.RecordLeaseAcquired();
            _logger.LogInformation("Claimed namespace {Namespace}", namespaceName);

            return store;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to set up namespace {Namespace} after lease acquire, releasing", namespaceName);

            await _pool.CloseAsync(namespaceName);
            await LeaseManager.ReleaseAsync(namespaceName, ct);

            return null;
        }
    }

    internal async Task<SlateDbStore?> AcceptAssignmentAsync(
        string namespaceName,
        string backend,
        CancellationToken ct = default)
    {
        if (_stopping)
        {
            return null;
        }

        await _claimLock.WaitAsync(ct);
        try
        {
            if (LeaseManager.IsOwnedLocally(namespaceName))
            {
                var store = _pool.TryGet(namespaceName);
                if (store != null)
                {
                    return store;
                }
            }

            if (!await LeaseManager.TryAcquireAsync(namespaceName, ct))
            {
                _logger.LogWarning("Failed to acquire lease for assigned {Namespace}", namespaceName);
                return null;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to accept assignment for {Namespace}", namespaceName);
            return null;
        }
        finally
        {
            _claimLock.Release();
        }

        try
        {
            var store = await _pool.GetOrOpenAsync(namespaceName, backend);
            _metrics.RecordLeaseAcquired();
            _logger.LogInformation("Accepted assignment for {Namespace}", namespaceName);

            return store;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open DB for assigned {Namespace}, releasing", namespaceName);
            await _pool.CloseAsync(namespaceName);

            await LeaseManager.ReleaseAsync(namespaceName, ct);

            return null;
        }
    }

    internal async Task ReleaseNamespaceAsync(string namespaceName)
    {
        await _pool.CloseAsync(namespaceName);

        if (_clusterConfig.ClusterMode)
        {
            await LeaseManager.ReleaseAsync(namespaceName);
        }

        _metrics.RecordLeaseReleased();
        _logger.LogInformation("Released namespace {Namespace}", namespaceName);
    }

    public async ValueTask DisposeAsync()
    {
        if (_renewTimer != null)
        {
            await _renewTimer.DisposeAsync();
        }

        _claimLock.Dispose();
    }

    private async Task ClaimUnclaimedNamespacesAsync(CancellationToken ct = default)
    {
        var namespaces = await _registry.ListAsync(ct);
        foreach (var ns in namespaces)
        {
            if (LeaseManager.IsOwnedLocally(ns.Name))
            {
                continue;
            }

            var owner = await LeaseManager.GetOwnerAsync(ns.Name, ct);
            if (owner != null)
            {
                continue;
            }

            await TryClaimAsync(ns.Name, ct);
        }
    }

    private async Task<bool> TryDrainViaLeaderAsync(CancellationToken ct)
    {
        try
        {
            var timeout = TimeSpan.FromSeconds(_clusterConfig.DrainTimeoutSeconds);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);

            var request = new DrainRequest { NodeId = _nodeConfig.Id };
            var reply = await Nats.RequestAsync<string, string>(
                NatsSubjects.ClusterDrain,
                JsonSerializer.Serialize(request),
                cancellationToken: cts.Token);

            var response = JsonSerializer.Deserialize<DrainReply>(reply.Data!);
            if (response?.Success == true)
            {
                _logger.LogInformation(
                    "Drain complete: {Count} namespaces reassigned",
                    response.NamespacesReassigned);

                return true;
            }

            _logger.LogWarning("Drain request failed, falling back to direct release");
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Drain via leader failed, falling back to direct release");
            return false;
        }
    }

    private async Task RenewLeasesAsync()
    {
        try
        {
            foreach (var ns in LeaseManager.OwnedNamespaces.ToList())
            {
                try
                {
                    if (await LeaseManager.TryRenewAsync(ns))
                    {
                        _metrics.RecordLeaseRenewed();
                    }
                    else
                    {
                        _metrics.RecordLeaseLost();
                        _logger.LogWarning("Lost lease for {Namespace}, releasing resources", ns);
                        await ReleaseNamespaceAsync(ns);
                    }
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
}
