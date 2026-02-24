namespace Pulsy.EKV.Client.Models;

public sealed record NamespaceInfo
{
    public required string Name { get; init; }

    public string Backend { get; init; } = "default";
}
