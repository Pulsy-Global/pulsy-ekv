using System.Text.Json;
using Microsoft.Extensions.Options;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Configuration.Pool;
using Pulsy.EKV.Node.Models;
using Pulsy.EKV.Node.Storage;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Cluster.Status;

public sealed class NodeStatusPublisher : IHostedService, IDisposable
{
    private readonly NatsKVContext _kv;
    private readonly NatsHealthState _natsHealth;
    private readonly NodeConfig _nodeConfig;
    private readonly ClusterConfig _clusterConfig;
    private readonly PoolConfig _poolConfig;
    private readonly DatabasePool _pool;
    private readonly ILogger<NodeStatusPublisher> _logger;
    private INatsKVStore? _store;
    private CancellationTokenSource? _cts;
    private Task? _publishTask;

    public NodeStatusPublisher(
        NatsKVContext kv,
        NatsHealthState natsHealth,
        IOptions<NodeConfig> nodeConfig,
        IOptions<ClusterConfig> clusterConfig,
        IOptions<PoolConfig> poolConfig,
        DatabasePool pool,
        ILogger<NodeStatusPublisher> logger)
    {
        _kv = kv;
        _natsHealth = natsHealth;
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
                MaxAge = TimeSpan.FromSeconds(_clusterConfig.StatusTtlSeconds),
            },
            ct);
        _natsHealth.ReportSuccess();

        _cts = new CancellationTokenSource();
        var interval = TimeSpan.FromSeconds(_clusterConfig.StatusIntervalSeconds);
        _publishTask = RunPublishLoopAsync(interval, _cts.Token);

        _logger.LogInformation(
            "Node status publisher started (bucket: {Bucket}, interval: {Interval}s)",
            NatsBuckets.NodeStatus,
            interval.TotalSeconds);
    }

    public async Task StopAsync(CancellationToken ct)
    {
        _cts?.Cancel();

        if (_publishTask != null)
        {
            try
            {
                await _publishTask.WaitAsync(ct);
            }
            catch (OperationCanceledException)
            {
                // Shutdown deadline reached or the publisher loop observed cancellation.
            }
        }

        try
        {
            if (_store != null)
            {
                await _store.DeleteAsync(_nodeConfig.Id, cancellationToken: ct);
                _logger.LogInformation("Node status removed from cluster");
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to remove node status on shutdown");
        }
    }

    public void Dispose()
    {
        _cts?.Dispose();
    }

    private async Task RunPublishLoopAsync(TimeSpan interval, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await PublishStatusAsync(ct);
                await Task.Delay(interval, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown in progress, expected.
        }
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
            _natsHealth.ReportSuccess();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown in progress, expected.
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to publish node status");
        }
    }
}
