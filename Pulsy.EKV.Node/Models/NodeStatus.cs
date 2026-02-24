namespace Pulsy.EKV.Node.Models;

public sealed record NodeStatus
{
    public required string NodeId { get; init; }

    public required string GrpcEndpoint { get; init; }

    public long DiskFreeBytes { get; init; }

    public int OpenNamespaceCount { get; init; }

    public int MaxOpenNamespaces { get; init; }
}
