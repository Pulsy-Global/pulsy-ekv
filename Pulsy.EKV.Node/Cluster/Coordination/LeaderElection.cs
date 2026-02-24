using Microsoft.Extensions.Options;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Node.Configuration;

namespace Pulsy.EKV.Node.Cluster.Coordination;

public sealed class LeaderElection : IAsyncDisposable
{
    private const string LeaderKey = "leader";

    private readonly NatsKVContext _kv;
    private readonly TimeSpan _ttl;
    private readonly TimeSpan _renewInterval;
    private readonly ILogger<LeaderElection> _logger;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private readonly string _nodeId;
    private INatsKVStore? _store;
    private ulong _revision;
    private Timer? _renewTimer;
    private volatile bool _isLeader;

    public bool IsLeader => _isLeader;

    public string? CurrentLeaderId { get; private set; }

    public LeaderElection(
        NatsKVContext kv,
        IOptions<NodeConfig> nodeConfig,
        IOptions<ClusterConfig> clusterConfig,
        ILogger<LeaderElection> logger)
    {
        _kv = kv;
        _nodeId = nodeConfig.Value.Id;
        _ttl = TimeSpan.FromSeconds(clusterConfig.Value.LeaderTtlSeconds);
        _renewInterval = TimeSpan.FromSeconds(clusterConfig.Value.LeaderRenewSeconds);
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        await GetStoreAsync(ct);
        await TryCampaignAsync(ct);

        _renewTimer = new Timer(_ => _ = TickAsync(), null, _renewInterval, Timeout.InfiniteTimeSpan);
    }

    private async Task TickAsync()
    {
        try
        {
            if (_isLeader)
            {
                await TryRenewAsync();
            }
            else
            {
                await TryCampaignAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Leader election tick failed");
        }
        finally
        {
            _renewTimer?.Change(_renewInterval, Timeout.InfiniteTimeSpan);
        }
    }

    private async Task TryCampaignAsync(CancellationToken ct = default)
    {
        var store = await GetStoreAsync(ct);
        try
        {
            _revision = await store.CreateAsync(LeaderKey, _nodeId, cancellationToken: ct);
            _isLeader = true;
            CurrentLeaderId = _nodeId;
            _logger.LogInformation("Won leader election (rev={Rev})", _revision);
        }
        catch (NatsKVCreateException)
        {
            var entry = await store.TryGetEntryAsync<string>(LeaderKey, cancellationToken: ct);
            CurrentLeaderId = entry.Success ? entry.Value.Value : null;

            if (_isLeader)
            {
                _isLeader = false;
                _logger.LogWarning("Lost leadership");
            }
        }
    }

    private async Task TryRenewAsync()
    {
        var store = await GetStoreAsync();
        try
        {
            _revision = await store.UpdateAsync(LeaderKey, _nodeId, _revision);
        }
        catch (NatsKVWrongLastRevisionException)
        {
            _isLeader = false;
            CurrentLeaderId = null;
            _logger.LogWarning("Lost leadership (CAS conflict on renew)");
        }
    }

    public async Task StopAsync()
    {
        var timer = _renewTimer;
        _renewTimer = null;

        if (timer != null)
        {
            await timer.DisposeAsync();
        }

        if (_isLeader)
        {
            try
            {
                var store = await GetStoreAsync();
                await store.DeleteAsync(LeaderKey);
                _logger.LogInformation("Resigned leadership");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to resign leadership");
            }

            _isLeader = false;
        }
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
                new NatsKVConfig(NatsBuckets.ClusterLeader) { MaxAge = _ttl }, ct);

            return _store;
        }
        finally
        {
            _initLock.Release();
        }
    }

    public ValueTask DisposeAsync()
    {
        _initLock.Dispose();
        return ValueTask.CompletedTask;
    }
}
