using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Node.Configuration;

namespace Pulsy.EKV.Node.Cluster.Leasing;

public sealed class NatsLeaseManager : ILeaseManager
{
    private const string KeyPrefix = "ns.";

    private readonly NatsKVContext _kv;
    private readonly string _nodeId;
    private readonly string _endpoint;
    private readonly TimeSpan _leaseTtl;
    private readonly ILogger<NatsLeaseManager> _logger;
    private readonly ConcurrentDictionary<string, CachedLeaseState> _leases = new();
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private INatsKVStore? _store;

    public NatsLeaseManager(
        NatsKVContext kv,
        IOptions<NodeConfig> nodeConfig,
        IOptions<ClusterConfig> clusterConfig,
        ILogger<NatsLeaseManager> logger)
    {
        _kv = kv;
        _nodeId = nodeConfig.Value.Id;
        _endpoint = nodeConfig.Value.GrpcEndpoint;
        _leaseTtl = TimeSpan.FromSeconds(clusterConfig.Value.LeaseTtlSeconds);
        _logger = logger;
    }

    public IReadOnlyCollection<string> OwnedNamespaces => _leases.Keys.ToArray();

    public bool IsOwnedLocally(string namespaceName) => TryGetActiveLease(namespaceName, out _);

    public async Task<bool> TryAcquireAsync(string namespaceName, CancellationToken ct = default)
    {
        var store = await GetStoreAsync(ct);
        var key = KeyPrefix + namespaceName;
        var existing = await store.TryGetEntryAsync<string>(key, cancellationToken: ct);
        if (existing.Success)
        {
            return await TryAcquireExistingAsync(store, key, namespaceName, existing.Value, ct);
        }

        var lease = CreateLease();
        try
        {
            var rev = await store.CreateAsync(
                key,
                NamespaceLeaseCodec.Serialize(lease),
                cancellationToken: ct);

            RememberLease(namespaceName, rev, lease);
            _logger.LogInformation("Lease acquired for {Namespace} (rev={Rev})", namespaceName, rev);
            return true;
        }
        catch (NatsKVCreateException)
        {
            var raced = await store.TryGetEntryAsync<string>(key, cancellationToken: ct);
            return raced.Success
                && await TryAcquireExistingAsync(store, key, namespaceName, raced.Value, ct);
        }
    }

    public async Task<bool> TryRenewAsync(string namespaceName, CancellationToken ct = default)
    {
        if (!TryGetActiveLease(namespaceName, out var state))
        {
            return false;
        }

        var store = await GetStoreAsync(ct);
        try
        {
            var lease = CreateLease(state.AcquiredAtUtc);
            var newRev = await store.UpdateAsync(
                KeyPrefix + namespaceName,
                NamespaceLeaseCodec.Serialize(lease),
                state.Revision,
                cancellationToken: ct);

            RememberLease(namespaceName, newRev, lease);
            return true;
        }
        catch (NatsKVWrongLastRevisionException)
        {
            _leases.TryRemove(namespaceName, out _);
            _logger.LogWarning("Lease lost for {Namespace} (CAS conflict)", namespaceName);
            return false;
        }
        catch (NatsKVKeyNotFoundException)
        {
            _leases.TryRemove(namespaceName, out _);
            _logger.LogWarning("Lease lost for {Namespace} (key not found)", namespaceName);
            return false;
        }
    }

    public async Task<bool> ReleaseAsync(string namespaceName, CancellationToken ct = default)
    {
        if (!_leases.TryGetValue(namespaceName, out var state))
        {
            return false;
        }

        var store = await GetStoreAsync(ct);
        try
        {
            await store.DeleteAsync(
                KeyPrefix + namespaceName,
                new NatsKVDeleteOpts { Revision = state.Revision },
                cancellationToken: ct);

            _leases.TryRemove(namespaceName, out _);
            _logger.LogInformation("Lease released for {Namespace}", namespaceName);
            return true;
        }
        catch (NatsKVWrongLastRevisionException)
        {
            _logger.LogWarning("Lease release skipped for {Namespace} (CAS conflict)", namespaceName);
            _leases.TryRemove(namespaceName, out _);
            return false;
        }
        catch (NatsKVKeyNotFoundException)
        {
            _leases.TryRemove(namespaceName, out _);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to release lease for {Namespace}", namespaceName);
            _leases.TryRemove(namespaceName, out _);
            return false;
        }
    }

    public async Task<NamespaceLease?> GetActiveLeaseAsync(string namespaceName, CancellationToken ct = default)
    {
        var store = await GetStoreAsync(ct);
        var result = await store.TryGetEntryAsync<string>(KeyPrefix + namespaceName, cancellationToken: ct);
        if (!result.Success)
        {
            return null;
        }

        var lease = NamespaceLeaseCodec.Parse(result.Value.Value);
        return NamespaceLeaseCodec.IsActive(lease, DateTimeOffset.UtcNow) ? lease : null;
    }

    private async ValueTask<INatsKVStore> GetStoreAsync(CancellationToken ct = default)
    {
        if (_store is { } store)
        {
            return store;
        }

        await _initLock.WaitAsync(ct);
        try
        {
            if (_store is { } existing)
            {
                return existing;
            }

            _store = await _kv.CreateOrUpdateStoreAsync(
                new NatsKVConfig(NatsBuckets.NamespaceLeases) { MaxAge = _leaseTtl }, ct);

            _logger.LogInformation(
                "Lease store initialized (bucket: {Bucket}, ttl: {Ttl}s)",
                NatsBuckets.NamespaceLeases,
                _leaseTtl.TotalSeconds);

            return _store;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task<bool> TryAcquireExistingAsync(
        INatsKVStore store,
        string key,
        string namespaceName,
        NatsKVEntry<string> entry,
        CancellationToken ct)
    {
        var currentLease = NamespaceLeaseCodec.Parse(entry.Value);
        var currentOwner = currentLease?.NodeId;
        if (currentOwner != _nodeId && !NamespaceLeaseCodec.HasExpired(currentLease, DateTimeOffset.UtcNow))
        {
            return false;
        }

        var lease = CreateLease(currentOwner == _nodeId ? NamespaceLeaseCodec.GetAcquiredAt(currentLease) : null);
        try
        {
            var rev = await store.UpdateAsync(
                key,
                NamespaceLeaseCodec.Serialize(lease),
                entry.Revision,
                cancellationToken: ct);

            RememberLease(namespaceName, rev, lease);
            _logger.LogInformation("Lease acquired for {Namespace} by CAS refresh (rev={Rev})", namespaceName, rev);
            return true;
        }
        catch (NatsKVWrongLastRevisionException)
        {
            return false;
        }
        catch (NatsKVKeyNotFoundException)
        {
            return false;
        }
    }

    private NamespaceLease CreateLease(DateTimeOffset? acquiredAtUtc = null)
    {
        var now = DateTimeOffset.UtcNow;
        var expiresAt = now.Add(_leaseTtl);
        return new NamespaceLease
        {
            NodeId = _nodeId,
            Endpoint = _endpoint,
            AcquiredAtUtc = acquiredAtUtc ?? now,
            RenewedAtUtc = now,
            ExpiresAtUtc = expiresAt,
        };
    }

    private void RememberLease(string namespaceName, ulong revision, NamespaceLease lease)
    {
        var expiresAt = lease.ExpiresAtUtc == default
            ? DateTimeOffset.UtcNow.Add(_leaseTtl)
            : lease.ExpiresAtUtc;

        var acquiredAt = lease.AcquiredAtUtc == default
            ? DateTimeOffset.UtcNow
            : lease.AcquiredAtUtc;

        _leases[namespaceName] = new CachedLeaseState(revision, expiresAt, acquiredAt);
    }

    private bool TryGetActiveLease(string namespaceName, out CachedLeaseState state)
    {
        if (_leases.TryGetValue(namespaceName, out state) && state.ExpiresAtUtc > DateTimeOffset.UtcNow)
        {
            return true;
        }

        _leases.TryRemove(namespaceName, out _);
        state = default;
        return false;
    }

    private readonly record struct CachedLeaseState(
        ulong Revision,
        DateTimeOffset ExpiresAtUtc,
        DateTimeOffset AcquiredAtUtc);
}
