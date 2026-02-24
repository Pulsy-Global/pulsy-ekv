using System.Text.Json;
using Microsoft.Extensions.Options;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Configuration.Pool;
using Pulsy.EKV.Node.Models;
using Pulsy.EKV.Node.Storage;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Cluster.Coordination;

public sealed class NodeStatusPublisher : IHostedService, IDisposable
{
    private readonly NatsKVContext _kv;
    private readonly NodeConfig _nodeConfig;
    private readonly ClusterConfig _clusterConfig;
    private readonly PoolConfig _poolConfig;
    private readonly DatabasePool _pool;
    private readonly ILogger<NodeStatusPublisher> _logger;
    private INatsKVStore? _store;
    private Timer? _timer;
    private CancellationTokenSource? _cts;

    public NodeStatusPublisher(
        NatsKVContext kv,
        IOptions<NodeConfig> nodeConfig,
        IOptions<ClusterConfig> clusterConfig,
        IOptions<PoolConfig> poolConfig,
        DatabasePool pool,
        ILogger<NodeStatusPublisher> logger)
    {
        _kv = kv;
        _nodeConfig = nodeConfig.Value;
        _clusterConfig = clusterConfig.Value;
        _poolConfig = poolConfig.Value;
        _pool = pool;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        _store = await _kv.CreateOrUpdateStoreAsync(
            new NatsKVConfig(NatsBuckets.NodeStatus)
            {
                MaxAge = TimeSpan.FromSeconds(_clusterConfig.StatusTtlSeconds)
            },
            ct);

        _cts = new CancellationTokenSource();
        var token = _cts.Token;
        var interval = TimeSpan.FromSeconds(_clusterConfig.StatusIntervalSeconds);
        _timer = new Timer(_ => _ = PublishStatusAsync(token), null, TimeSpan.Zero, interval);

        _logger.LogInformation("Node status publisher started (bucket: {Bucket}, interval: {Interval}s)", NatsBuckets.NodeStatus, interval.TotalSeconds);
    }

    public Task StopAsync(CancellationToken ct)
    {
        _timer?.Change(Timeout.Infinite, Timeout.Infinite);
        _cts?.Cancel();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _timer?.Dispose();
        _cts?.Dispose();
    }

    private async Task PublishStatusAsync(CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
        {
            return;
        }

        try
        {
            var diskFreeBytes = DiskUtil.GetFreeBytes(_nodeConfig.DataPath);

            var status = new NodeStatus
            {
                NodeId = _nodeConfig.Id,
                GrpcEndpoint = _nodeConfig.GrpcEndpoint,
                DiskFreeBytes = diskFreeBytes,
                OpenNamespaceCount = _pool.OpenCount,
                MaxOpenNamespaces = _poolConfig.MaxOpen,
            };

            var json = JsonSerializer.Serialize(status);
            await _store!.PutAsync(_nodeConfig.Id, json, cancellationToken: ct);
        }
        catch (OperationCanceledException)
        {
            // Shutdown in progress, expected.
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to publish node status");
        }
    }
}
