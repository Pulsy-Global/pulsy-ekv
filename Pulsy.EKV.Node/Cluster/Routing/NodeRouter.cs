using System.Collections.Concurrent;
using System.Text.Json;
using Grpc.Net.Client;
using Microsoft.Extensions.Options;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Grpc;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Models;
using Pulsy.EKV.Node.Storage;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Cluster.Routing;

public sealed class NodeRouter : IAsyncDisposable, IHostedService
{
    private readonly DatabasePool _pool;
    private readonly ILeaseManager _leaseManager;
    private readonly NatsKVContext _kv;
    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new();
    private readonly ILogger<NodeRouter> _logger;
    private readonly int _maxGrpcMessageBytes;
    private Timer? _cleanupTimer;

    public NodeRouter(
        DatabasePool pool,
        ILeaseManager leaseManager,
        NatsKVContext kv,
        IOptions<LimitsConfig> limits,
        IOptions<ClusterConfig> clusterConfig,
        ILogger<NodeRouter> logger)
    {
        _pool = pool;
        _leaseManager = leaseManager;
        _kv = kv;
        _logger = logger;
        _maxGrpcMessageBytes = limits.Value.MaxGrpcMessageBytes;

        var interval = TimeSpan.FromSeconds(clusterConfig.Value.StatusIntervalSeconds * 3);
        _cleanupTimer = new Timer(_ => EvictStaleChannels(), null, interval, interval);
    }

    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;

    public SlateDbStore? GetLocalStore(string namespaceName) => _pool.TryGet(namespaceName);

    public async Task StopAsync(CancellationToken ct)
    {
        if (_cleanupTimer != null)
        {
            await _cleanupTimer.DisposeAsync();
        }

        _cleanupTimer = null;
    }

    public async Task<EkvStore.EkvStoreClient?> GetForwardingClientAsync(
        string namespaceName,
        CancellationToken ct = default)
    {
        var owner = await _leaseManager.GetOwnerAsync(namespaceName, ct);
        if (owner == null)
        {
            _logger.LogWarning("No owner found for namespace {Namespace}", namespaceName);
            return null;
        }

        var endpoint = await ResolveEndpointAsync(owner, ct);
        if (endpoint == null)
        {
            _logger.LogWarning("No endpoint found for node {Node}", owner);
            return null;
        }

        return new EkvStore.EkvStoreClient(GetOrCreateChannel(endpoint));
    }

    public async Task<EkvAdmin.EkvAdminClient?> GetForwardingAdminClientAsync(
        CancellationToken ct = default)
    {
        var endpoint = await GetAnyNodeEndpointAsync(ct);
        if (endpoint == null)
        {
            return null;
        }

        return new EkvAdmin.EkvAdminClient(GetOrCreateChannel(endpoint));
    }

    public async ValueTask DisposeAsync()
    {
        if (_cleanupTimer != null)
        {
            await _cleanupTimer.DisposeAsync();
        }

        foreach (var channel in _channels.Values)
        {
            channel.Dispose();
        }

        _channels.Clear();
    }

    private GrpcChannel GetOrCreateChannel(string endpoint) =>
        _channels.GetOrAdd(endpoint, ep => GrpcChannel.ForAddress(ep, new GrpcChannelOptions
        {
            MaxReceiveMessageSize = _maxGrpcMessageBytes,
            MaxSendMessageSize = _maxGrpcMessageBytes,
        }));

    private async Task<string?> ResolveEndpointAsync(string nodeId, CancellationToken ct)
    {
        try
        {
            var store = await _kv.GetStoreAsync(NatsBuckets.NodeStatus, ct);
            var result = await store.TryGetEntryAsync<string>(nodeId, cancellationToken: ct);
            if (!result.Success)
            {
                return null;
            }

            var status = JsonSerializer.Deserialize<NodeStatus>(result.Value.Value!);
            return status?.GrpcEndpoint;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to resolve endpoint for node {Node}", nodeId);
            return null;
        }
    }

    private async Task<string?> GetAnyNodeEndpointAsync(CancellationToken ct)
    {
        try
        {
            var store = await _kv.GetStoreAsync(NatsBuckets.NodeStatus, ct);
            await foreach (var key in store.GetKeysAsync(cancellationToken: ct))
            {
                var result = await store.TryGetEntryAsync<string>(key, cancellationToken: ct);
                if (!result.Success)
                {
                    continue;
                }

                var status = JsonSerializer.Deserialize<NodeStatus>(result.Value.Value!);
                if (status?.GrpcEndpoint != null)
                {
                    return status.GrpcEndpoint;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to find any node endpoint");
        }

        return null;
    }

    private async void EvictStaleChannels() => await EvictStaleChannelsAsync();

    private async Task EvictStaleChannelsAsync()
    {
        try
        {
            var liveEndpoints = new HashSet<string>();
            var store = await _kv.GetStoreAsync(NatsBuckets.NodeStatus);
            await foreach (var key in store.GetKeysAsync())
            {
                var result = await store.TryGetEntryAsync<string>(key);
                if (!result.Success)
                {
                    continue;
                }

                var status = JsonSerializer.Deserialize<NodeStatus>(result.Value.Value!);
                if (status?.GrpcEndpoint != null)
                {
                    liveEndpoints.Add(status.GrpcEndpoint);
                }
            }

            foreach (var endpoint in _channels.Keys)
            {
                if (liveEndpoints.Contains(endpoint))
                {
                    continue;
                }

                if (_channels.TryRemove(endpoint, out var channel))
                {
                    _logger.LogInformation("Evicting stale gRPC channel to {Endpoint}", endpoint);
                    channel.Dispose();
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to evict stale channels");
        }
    }
}
