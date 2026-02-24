namespace Pulsy.EKV.Node.Models;

public sealed record NamespaceConfig
{
    public required string Name { get; init; }

    public string Backend { get; init; } = "local";
}
