using System.Collections.Concurrent;
using System.Text.Json;
using Grpc.Net.Client;
using Microsoft.Extensions.Options;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Grpc;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Models;

namespace Pulsy.EKV.Node.Cluster.Routing;

public sealed class NodeRouter : IAsyncDisposable, IHostedService
{
    private readonly ILeaseManager _leaseManager;
    private readonly NatsKVContext _kv;
    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new();
    private readonly ILogger<NodeRouter> _logger;
    private readonly string _nodeId;
    private readonly string _localEndpoint;
    private readonly int _maxGrpcMessageBytes;
    private readonly TimeSpan _cleanupInterval;
    private CancellationTokenSource? _cleanupCts;
    private Task? _cleanupTask;
    private int _disposeState;

    public NodeRouter(
        ILeaseManager leaseManager,
        NatsKVContext kv,
        IOptions<LimitsConfig> limits,
        IOptions<ClusterConfig> clusterConfig,
        IOptions<NodeConfig> nodeConfig,
        ILogger<NodeRouter> logger)
    {
        _leaseManager = leaseManager;
        _kv = kv;
        _logger = logger;
        _nodeId = nodeConfig.Value.Id;
        _localEndpoint = NormalizeEndpoint(nodeConfig.Value.GrpcEndpoint);
        _maxGrpcMessageBytes = limits.Value.MaxGrpcMessageBytes;
        _cleanupInterval = TimeSpan.FromSeconds(clusterConfig.Value.StatusIntervalSeconds * 3);
    }

    public Task StartAsync(CancellationToken ct)
    {
        _cleanupCts = new CancellationTokenSource();
        _cleanupTask = RunCleanupLoopAsync(_cleanupCts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken ct)
    {
        var cleanupCts = Interlocked.Exchange(ref _cleanupCts, null);
        var cleanupTask = Interlocked.Exchange(ref _cleanupTask, null);
        cleanupCts?.Cancel();

        if (cleanupTask != null)
        {
            try
            {
                await cleanupTask.WaitAsync(ct);
            }
            catch (OperationCanceledException)
            {
                // Shutdown deadline reached or the cleanup loop observed cancellation.
            }
        }

        cleanupCts?.Dispose();
    }

    public async Task<EkvStore.EkvStoreClient?> GetForwardingClientAsync(
        string namespaceName,
        CancellationToken ct = default)
    {
        var lease = await _leaseManager.GetActiveLeaseAsync(namespaceName, ct);
        if (lease == null)
        {
            _logger.LogWarning("No owner found for namespace {Namespace}", namespaceName);
            return null;
        }

        var owner = lease.NodeId;
        var endpoint = lease.Endpoint;
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            _logger.LogWarning(
                "Lease for namespace {Namespace} owned by node {Node} has no endpoint",
                namespaceName,
                owner);
            return null;
        }

        if (owner == _nodeId || string.Equals(NormalizeEndpoint(endpoint), _localEndpoint, StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "Refusing to forward namespace {Namespace} to stale local owner {Owner} at {Endpoint}",
                namespaceName,
                owner,
                endpoint);
            return null;
        }

        return new EkvStore.EkvStoreClient(GetOrCreateChannel(endpoint));
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeState, 1) != 0)
        {
            return;
        }

        var cleanupCts = Interlocked.Exchange(ref _cleanupCts, null);
        var cleanupTask = Interlocked.Exchange(ref _cleanupTask, null);
        cleanupCts?.Cancel();

        if (cleanupTask != null)
        {
            try
            {
                await cleanupTask;
            }
            catch (OperationCanceledException)
            {
                // Shutdown in progress, expected.
            }
        }

        cleanupCts?.Dispose();

        foreach (var channel in _channels.Values)
        {
            channel.Dispose();
        }

        _channels.Clear();
    }

    private static string NormalizeEndpoint(string endpoint)
    {
        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri))
        {
            return endpoint.TrimEnd('/');
        }

        return uri.GetComponents(UriComponents.SchemeAndServer, UriFormat.Unescaped);
    }

    private GrpcChannel GetOrCreateChannel(string endpoint)
    {
        var normalizedEndpoint = NormalizeEndpoint(endpoint);
        return _channels.GetOrAdd(normalizedEndpoint, ep => GrpcChannel.ForAddress(ep, new GrpcChannelOptions
        {
            MaxReceiveMessageSize = _maxGrpcMessageBytes,
            MaxSendMessageSize = _maxGrpcMessageBytes,
        }));
    }

    private async Task RunCleanupLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_cleanupInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                await EvictStaleChannelsAsync(ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown in progress, expected.
        }
    }

    private async Task EvictStaleChannelsAsync(CancellationToken ct)
    {
        try
        {
            var liveEndpoints = new HashSet<string>();
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
                    liveEndpoints.Add(NormalizeEndpoint(status.GrpcEndpoint));
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
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown in progress, expected.
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to evict stale channels");
        }
    }
}
