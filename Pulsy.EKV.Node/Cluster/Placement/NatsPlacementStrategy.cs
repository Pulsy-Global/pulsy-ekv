using System.Text.Json;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Node.Models;

namespace Pulsy.EKV.Node.Cluster.Placement;

public sealed class NatsPlacementStrategy : IPlacementStrategy
{
    private readonly NatsKVContext _kv;
    private readonly ILogger<NatsPlacementStrategy> _logger;
    private INatsKVStore? _store;

    public NatsPlacementStrategy(NatsKVContext kv, ILogger<NatsPlacementStrategy> logger)
    {
        _kv = kv;
        _logger = logger;
    }

    public async Task InitAsync(CancellationToken ct = default)
    {
        _store = await _kv.CreateOrUpdateStoreAsync(new NatsKVConfig(NatsBuckets.NodeStatus), ct);
    }

    public async Task<string?> SelectNodeAsync(CancellationToken ct = default)
    {
        string? bestNode = null;
        double bestScore = double.MinValue;

        await foreach (var key in _store!.GetKeysAsync(cancellationToken: ct))
        {
            var result = await _store.TryGetEntryAsync<string>(key, cancellationToken: ct);
            if (!result.Success)
            {
                continue;
            }

            var status = JsonSerializer.Deserialize<NodeStatus>(result.Value.Value!);
            if (status == null)
            {
                continue;
            }

            if (status.OpenNamespaceCount >= status.MaxOpenNamespaces)
            {
                continue;
            }

            var score = ScoreNode(status);
            if (score > bestScore)
            {
                bestScore = score;
                bestNode = status.NodeId;
            }
        }

        _logger.LogDebug("Placement selected node {Node} (score={Score:F2})", bestNode, bestScore);

        return bestNode;
    }

    /// <summary>
    /// Scores a node for placement. Higher is better.
    /// Returns free disk per namespace slot
    /// </summary>
    private static double ScoreNode(NodeStatus status)
    {
        return status.DiskFreeBytes / (double)(status.OpenNamespaceCount + 1);
    }
}
