using System.Text.Json;

namespace Pulsy.EKV.Node.Cluster.Leasing;

internal static class NamespaceLeaseCodec
{
    public static string Serialize(NamespaceLease lease) => JsonSerializer.Serialize(lease);

    public static NamespaceLease? Parse(string? value)
    {
        var payload = value.AsSpan().Trim();
        if (payload.IsEmpty)
        {
            return null;
        }

        return ParsePayload(value!);
    }

    public static bool IsActive(NamespaceLease? lease, DateTimeOffset now)
        => lease != null
            && !string.IsNullOrWhiteSpace(lease.NodeId)
            && !HasExpired(lease, now);

    public static bool HasExpired(NamespaceLease? lease, DateTimeOffset now)
        => lease is { ExpiresAtUtc: var expiresAt }
            && expiresAt != default
            && expiresAt <= now;

    public static DateTimeOffset? GetAcquiredAt(NamespaceLease? lease)
    {
        if (lease == null || lease.AcquiredAtUtc == default)
        {
            return null;
        }

        return lease.AcquiredAtUtc;
    }

    private static NamespaceLease? ParsePayload(string value)
    {
        try
        {
            return JsonSerializer.Deserialize<NamespaceLease>(value);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
