namespace Pulsy.EKV.Client.Models;

public sealed record ScanResult
{
    public required IReadOnlyList<KvEntry> Items { get; init; }

    public string? NextCursor { get; init; }

    public bool HasMore { get; init; }
}
