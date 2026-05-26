namespace Pulsy.EKV.Node.Cluster.Leasing;

public interface ILeaseManager
{
    IReadOnlyCollection<string> OwnedNamespaces { get; }

    Task<bool> TryAcquireAsync(string namespaceName, CancellationToken ct = default);

    Task<bool> TryRenewAsync(string namespaceName, CancellationToken ct = default);

    Task<bool> ReleaseAsync(string namespaceName, CancellationToken ct = default);

    Task<NamespaceLease?> GetActiveLeaseAsync(string namespaceName, CancellationToken ct = default);

    bool IsOwnedLocally(string namespaceName);
}
