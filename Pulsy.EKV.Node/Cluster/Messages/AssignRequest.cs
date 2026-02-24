namespace Pulsy.EKV.Node.Cluster.Messages;

public sealed record AssignRequest
{
    public required string Namespace { get; init; }

    public required string Backend { get; init; }
}
