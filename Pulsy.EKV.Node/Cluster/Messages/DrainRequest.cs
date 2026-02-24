namespace Pulsy.EKV.Node.Cluster.Messages;

public sealed record DrainRequest
{
    public required string NodeId { get; init; }
}
