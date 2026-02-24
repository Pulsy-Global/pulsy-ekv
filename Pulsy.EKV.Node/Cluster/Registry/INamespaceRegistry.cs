using Pulsy.EKV.Node.Models;

namespace Pulsy.EKV.Node.Cluster.Registry;

public interface INamespaceRegistry
{
    Task InitAsync(CancellationToken ct = default);

    Task<NamespaceConfig?> GetAsync(string name, CancellationToken ct = default);

    Task<IReadOnlyList<NamespaceConfig>> ListAsync(CancellationToken ct = default);

    Task CreateAsync(NamespaceConfig config, CancellationToken ct = default);

    Task UpdateAsync(NamespaceConfig config, CancellationToken ct = default);

    Task DeleteAsync(string name, CancellationToken ct = default);
}
