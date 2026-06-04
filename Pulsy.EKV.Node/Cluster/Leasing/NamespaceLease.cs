using System.Text.Json.Serialization;

namespace Pulsy.EKV.Node.Cluster.Leasing;

public sealed record NamespaceLease
{
    [JsonPropertyName("nodeId")]
    public string NodeId { get; init; } = string.Empty;

    [JsonPropertyName("endpoint")]
    public string Endpoint { get; init; } = string.Empty;

    [JsonPropertyName("acquiredAtUtc")]
    public DateTimeOffset AcquiredAtUtc { get; init; }

    [JsonPropertyName("renewedAtUtc")]
    public DateTimeOffset RenewedAtUtc { get; init; }

    [JsonPropertyName("expiresAtUtc")]
    public DateTimeOffset ExpiresAtUtc { get; init; }
}
