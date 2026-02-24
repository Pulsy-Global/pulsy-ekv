using System.Text.Json;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Node.Cluster.Messages;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Configuration;

namespace Pulsy.EKV.Node.Cluster.Coordination;

public sealed class ClusterCoordinator : BackgroundService
{
    private readonly LeaderElection _election;
    private readonly NamespaceCoordinator _namespaceCoordinator;
    private readonly NamespaceRedistributor _redistributor;
    private readonly INatsConnection _nats;
    private readonly NatsKVContext _kv;
    private readonly ClusterConfig _clusterConfig;
    private readonly ILogger<ClusterCoordinator> _logger;
    private INatsKVStore? _nodeStatusStore;

    private readonly string _nodeId;
    private readonly HashSet<string> _knownNodes = new();
    private readonly SemaphoreSlim _storeInitLock = new(1, 1);

    private volatile INatsSub<string>? _drainSub;
    private volatile bool _drainSubStarting;

    public ClusterCoordinator(
        LeaderElection election,
        NamespaceCoordinator namespaceCoordinator,
        NamespaceRedistributor redistributor,
        INatsConnection nats,
        NatsKVContext kv,
        IOptions<NodeConfig> nodeConfig,
        IOptions<ClusterConfig> clusterConfig,
        ILogger<ClusterCoordinator> logger)
    {
        _election = election;
        _namespaceCoordinator = namespaceCoordinator;
        _redistributor = redistributor;
        _nats = nats;
        _kv = kv;
        _nodeId = nodeConfig.Value.Id;
        _clusterConfig = clusterConfig.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await _election.StartAsync(ct);

        var assignTask = ListenForAssignmentsAsync(ct);
        var monitorTask = MonitorClusterHealthAsync(ct);

        await Task.WhenAll(assignTask, monitorTask);
    }

    public override async Task StopAsync(CancellationToken ct)
    {
        await _election.StopAsync();
        await base.StopAsync(ct);
    }

    private async Task ListenForAssignmentsAsync(CancellationToken ct)
    {
        var subject = NatsSubjects.AssignNode(_nodeId);
        await using var sub = await _nats.SubscribeCoreAsync<string>(subject, cancellationToken: ct);
        _logger.LogInformation("Listening for assignments on {Subject}", subject);

        await foreach (var msg in sub.Msgs.ReadAllAsync(ct))
        {
            try
            {
                var request = JsonSerializer.Deserialize<AssignRequest>(msg.Data!);
                if (request == null)
                {
                    await msg.ReplyAsync(
                        JsonSerializer.Serialize(new AssignReply { Success = false, Error = "Invalid request" }),
                        cancellationToken: ct);
                    continue;
                }

                _logger.LogInformation("Received assignment for {Namespace}", request.Namespace);

                var store = await _namespaceCoordinator.AcceptAssignmentAsync(request.Namespace, request.Backend, ct);
                await msg.ReplyAsync(
                    JsonSerializer.Serialize(new AssignReply { Success = store != null }),
                    cancellationToken: ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling assignment");
                try
                {
                    await msg.ReplyAsync(
                        JsonSerializer.Serialize(new AssignReply { Success = false, Error = ex.Message }),
                        cancellationToken: ct);
                }
                catch (Exception replyEx)
                {
                    _logger.LogDebug(replyEx, "Failed to send error reply for assignment");
                }
            }
        }
    }

    private async Task MonitorClusterHealthAsync(CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_clusterConfig.ClusterPollSeconds);

        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(interval, ct);

            if (_election.IsLeader)
            {
                if (_drainSub == null && !_drainSubStarting)
                {
                    _drainSubStarting = true;
                    _ = StartDrainSubscriptionAsync(ct);
                }

                try
                {
                    await CheckClusterHealthAsync(ct);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error during cluster health check");
                }
            }
            else
            {
                if (_drainSub != null)
                {
                    await StopDrainSubscriptionAsync();
                }

                _knownNodes.Clear();
            }
        }
    }

    private async Task StartDrainSubscriptionAsync(CancellationToken ct)
    {
        try
        {
            _drainSub = await _nats.SubscribeCoreAsync<string>(
                NatsSubjects.ClusterDrain, cancellationToken: ct);
        }
        catch (Exception ex)
        {
            _drainSubStarting = false;
            _logger.LogError(ex, "Failed to start drain subscription");
            return;
        }

        _drainSubStarting = false;
        _logger.LogInformation("Leader: listening for drain requests");

        await foreach (var msg in _drainSub.Msgs.ReadAllAsync(ct))
        {
            try
            {
                var request = JsonSerializer.Deserialize<DrainRequest>(msg.Data!);
                if (request == null)
                {
                    continue;
                }

                _logger.LogInformation("Drain request from node {Node}", request.NodeId);
                var count = await _redistributor.RedistributeNodeAsync(request.NodeId, ct);

                await msg.ReplyAsync(
                    JsonSerializer.Serialize(new DrainReply { Success = true, NamespacesReassigned = count }),
                    cancellationToken: ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling drain request");
                try
                {
                    await msg.ReplyAsync(
                        JsonSerializer.Serialize(new DrainReply { Success = false }),
                        cancellationToken: ct);
                }
                catch (Exception replyEx)
                {
                    _logger.LogDebug(replyEx, "Failed to send drain error reply");
                }
            }
        }
    }

    private async Task StopDrainSubscriptionAsync()
    {
        var sub = _drainSub;
        _drainSub = null;
        _drainSubStarting = false;

        if (sub != null)
        {
            await sub.DisposeAsync();
            _logger.LogInformation("Stopped drain subscription (no longer leader)");
        }
    }

    private async Task CheckClusterHealthAsync(CancellationToken ct)
    {
        var currentNodes = new HashSet<string>();

        var store = await GetNodeStatusStoreAsync(ct);

        await foreach (var key in store.GetKeysAsync(cancellationToken: ct))
        {
            currentNodes.Add(key);
        }

        if (_knownNodes.Count == 0)
        {
            foreach (var node in currentNodes)
            {
                _knownNodes.Add(node);
            }

            _logger.LogInformation("Initial cluster scan: {Count} nodes", _knownNodes.Count);
            return;
        }

        var deadNodes = _knownNodes.Except(currentNodes).ToList();

        foreach (var deadNode in deadNodes)
        {
            _logger.LogWarning("Node {Node} disappeared from cluster", deadNode);
            _knownNodes.Remove(deadNode);

            try
            {
                await _redistributor.RedistributeNodeAsync(deadNode, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to redistribute namespaces of dead node {Node}", deadNode);
            }
        }

        foreach (var node in currentNodes)
        {
            _knownNodes.Add(node);
        }
    }

    private async ValueTask<INatsKVStore> GetNodeStatusStoreAsync(CancellationToken ct = default)
    {
        if (_nodeStatusStore is { } store)
        {
            return store;
        }

        await _storeInitLock.WaitAsync(ct);
        try
        {
            if (_nodeStatusStore is { } existing)
            {
                return existing;
            }

            _nodeStatusStore = await _kv.CreateOrUpdateStoreAsync(
                new NatsKVConfig(NatsBuckets.NodeStatus),
                ct);

            return _nodeStatusStore;
        }
        finally
        {
            _storeInitLock.Release();
        }
    }
}
