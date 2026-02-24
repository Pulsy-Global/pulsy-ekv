namespace Pulsy.EKV.Node.Cluster.Messages;

public sealed record AssignReply
{
    public bool Success { get; init; }

    public string? Error { get; init; }
}
