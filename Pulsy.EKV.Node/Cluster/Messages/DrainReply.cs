namespace Pulsy.EKV.Node.Cluster.Messages;

public sealed record DrainReply
{
    public bool Success { get; init; }

    public int NamespacesReassigned { get; init; }
}
