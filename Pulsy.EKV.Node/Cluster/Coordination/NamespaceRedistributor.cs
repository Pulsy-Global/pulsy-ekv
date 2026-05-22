using System.Text.Json;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Cluster.Messages;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Cluster.Placement;
using Pulsy.EKV.Node.Cluster.Registry;
using Pulsy.EKV.Node.Configuration;

namespace Pulsy.EKV.Node.Cluster.Coordination;

public sealed class NamespaceRedistributor
{
    private readonly ILeaseManager _leaseManager;
    private readonly INamespaceRegistry _registry;
    private readonly IPlacementStrategy _placement;
    private readonly NamespaceCoordinator _namespaceCoordinator;
    private readonly INatsConnection _nats;
    private readonly string _nodeId;
    private readonly ILogger<NamespaceRedistributor> _logger;

    private readonly SemaphoreSlim _lock = new(1, 1);

    public NamespaceRedistributor(
        ILeaseManager leaseManager,
        INamespaceRegistry registry,
        IPlacementStrategy placement,
        NamespaceCoordinator namespaceCoordinator,
        INatsConnection nats,
        IOptions<NodeConfig> nodeConfig,
        ILogger<NamespaceRedistributor> logger)
    {
        _leaseManager = leaseManager;
        _registry = registry;
        _placement = placement;
        _namespaceCoordinator = namespaceCoordinator;
        _nats = nats;
        _nodeId = nodeConfig.Value.Id;
        _logger = logger;
    }

    public async Task<int> RedistributeNodeAsync(string deadNodeId, CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            var namespaces = await GetRedistributionCandidatesAsync(deadNodeId, ct);
            if (namespaces.Count == 0)
            {
                _logger.LogInformation("No namespaces to redistribute for node {Node}", deadNodeId);
                return 0;
            }

            _logger.LogInformation(
                "Redistributing {Count} namespaces from node {Node}",
                namespaces.Count,
                deadNodeId);

            var reassigned = 0;

            foreach (var candidate in namespaces)
            {
                var ns = candidate.Namespace;

                try
                {
                    var config = await _registry.GetAsync(ns, ct);
                    if (config == null)
                    {
                        _logger.LogWarning("Namespace {Namespace} not found in registry, skipping", ns);
                        continue;
                    }

                    var targetNode = await _placement.SelectNodeAsync(ct);
                    if (targetNode == null || targetNode == deadNodeId)
                    {
                        _logger.LogWarning("No suitable target node for {Namespace}, skipping", ns);
                        continue;
                    }

                    if (targetNode == _nodeId)
                    {
                        var store = await _namespaceCoordinator.AcceptAssignmentAsync(
                            ns,
                            config.Backend,
                            candidate.ExpectedOwner,
                            ct);
                        if (store != null)
                        {
                            reassigned++;
                        }

                        continue;
                    }

                    var request = new AssignRequest
                    {
                        Namespace = ns,
                        Backend = config.Backend,
                        ExpectedOwner = candidate.ExpectedOwner,
                    };

                    var reply = await _nats.RequestAsync<string, string>(
                        NatsSubjects.AssignNode(targetNode),
                        JsonSerializer.Serialize(request),
                        cancellationToken: ct);

                    var response = JsonSerializer.Deserialize<AssignReply>(reply.Data!);
                    if (response?.Success == true)
                    {
                        reassigned++;
                        _logger.LogInformation("Reassigned {Namespace} to node {Node}", ns, targetNode);
                    }
                    else
                    {
                        _logger.LogWarning(
                            "Failed to assign {Namespace} to {Node}: {Error}",
                            ns,
                            targetNode,
                            response?.Error);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error redistributing namespace {Namespace}", ns);
                }
            }

            _logger.LogInformation(
                "Redistribution complete: {Reassigned}/{Total} namespaces from node {Node}",
                reassigned,
                namespaces.Count,
                deadNodeId);

            return reassigned;
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<IReadOnlyList<RedistributionCandidate>> GetRedistributionCandidatesAsync(
        string deadNodeId,
        CancellationToken ct)
    {
        var candidates = new Dictionary<string, RedistributionCandidate>(StringComparer.Ordinal);

        foreach (var ns in await _leaseManager.GetNamespacesByOwnerAsync(deadNodeId, ct))
        {
            candidates[ns] = new RedistributionCandidate(ns, deadNodeId);
        }

        var namespaces = await _registry.ListAsync(ct);
        foreach (var ns in namespaces)
        {
            var owner = await _leaseManager.GetOwnerAsync(ns.Name, ct);
            if (owner == null)
            {
                candidates[ns.Name] = new RedistributionCandidate(ns.Name, null);
            }
            else if (owner == deadNodeId)
            {
                candidates[ns.Name] = new RedistributionCandidate(ns.Name, deadNodeId);
            }
        }

        return candidates.Values.ToList();
    }

    private sealed record RedistributionCandidate(string Namespace, string? ExpectedOwner);
}
