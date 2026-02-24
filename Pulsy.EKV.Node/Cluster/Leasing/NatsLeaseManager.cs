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
    private readonly TimeSpan _leaseTtl;
    private readonly ILogger<NatsLeaseManager> _logger;
    private readonly ConcurrentDictionary<string, ulong> _revisions = new();
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
        _leaseTtl = TimeSpan.FromSeconds(clusterConfig.Value.LeaseTtlSeconds);
        _logger = logger;
    }

    public IReadOnlyCollection<string> OwnedNamespaces => _revisions.Keys.ToArray();

    public bool IsOwnedLocally(string namespaceName) => _revisions.ContainsKey(namespaceName);

    public async Task<bool> TryAcquireAsync(string namespaceName, CancellationToken ct = default)
    {
        var store = await GetStoreAsync(ct);
        try
        {
            var rev = await store.CreateAsync(KeyPrefix + namespaceName, _nodeId, cancellationToken: ct);
            _revisions[namespaceName] = rev;
            _logger.LogInformation("Lease acquired for {Namespace} (rev={Rev})", namespaceName, rev);
            return true;
        }
        catch (NatsKVCreateException)
        {
            return false;
        }
    }

    public async Task<bool> TryRenewAsync(string namespaceName, CancellationToken ct = default)
    {
        if (!_revisions.TryGetValue(namespaceName, out var expectedRev))
        {
            return false;
        }

        var store = await GetStoreAsync(ct);
        try
        {
            var newRev = await store.UpdateAsync(
                KeyPrefix + namespaceName,
                _nodeId,
                expectedRev,
                cancellationToken: ct);

            _revisions[namespaceName] = newRev;
            return true;
        }
        catch (NatsKVWrongLastRevisionException)
        {
            _revisions.TryRemove(namespaceName, out _);
            _logger.LogWarning("Lease lost for {Namespace} (CAS conflict)", namespaceName);
            return false;
        }
    }

    public async Task<bool> ReleaseAsync(string namespaceName, CancellationToken ct = default)
    {
        var store = await GetStoreAsync(ct);
        try
        {
            await store.DeleteAsync(KeyPrefix + namespaceName, cancellationToken: ct);
            _revisions.TryRemove(namespaceName, out _);
            _logger.LogInformation("Lease released for {Namespace}", namespaceName);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to release lease for {Namespace}", namespaceName);
            _revisions.TryRemove(namespaceName, out _);
            return false;
        }
    }

    public async Task<string?> GetOwnerAsync(string namespaceName, CancellationToken ct = default)
    {
        var store = await GetStoreAsync(ct);
        var result = await store.TryGetEntryAsync<string>(KeyPrefix + namespaceName, cancellationToken: ct);
        return result.Success ? result.Value.Value : null;
    }

    public async Task<IReadOnlyList<string>> GetNamespacesByOwnerAsync(string nodeId, CancellationToken ct = default)
    {
        var store = await GetStoreAsync(ct);
        var result = new List<string>();

        await foreach (var key in store.GetKeysAsync(cancellationToken: ct))
        {
            var entry = await store.TryGetEntryAsync<string>(key, cancellationToken: ct);
            if (entry.Success && entry.Value.Value == nodeId)
            {
                var ns = key.StartsWith(KeyPrefix) ? key[KeyPrefix.Length..] : key;
                result.Add(ns);
            }
        }

        return result;
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
}
