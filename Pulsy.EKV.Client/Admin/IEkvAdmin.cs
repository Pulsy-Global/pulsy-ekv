using Pulsy.EKV.Client.Models;

namespace Pulsy.EKV.Client.Admin;

public interface IEkvAdmin
{
    Task CreateNamespaceAsync(NamespaceInfo config, CancellationToken ct = default);

    Task EnsureNamespaceAsync(NamespaceInfo config, CancellationToken ct = default);

    Task<NamespaceInfo?> GetNamespaceAsync(string name, CancellationToken ct = default);

    Task<IReadOnlyList<NamespaceInfo>> ListNamespacesAsync(CancellationToken ct = default);

    Task UpdateNamespaceAsync(NamespaceInfo config, CancellationToken ct = default);

    Task HibernateNamespaceAsync(string name, CancellationToken ct = default);

    Task DeleteNamespaceAsync(string name, CancellationToken ct = default);
}
