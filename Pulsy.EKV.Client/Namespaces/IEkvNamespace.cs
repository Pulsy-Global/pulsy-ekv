using Pulsy.EKV.Client.Models;
using Pulsy.EKV.Client.Operations;

namespace Pulsy.EKV.Client.Namespaces;

public interface IEkvNamespace
{
    Task<byte[]?> GetAsync(string key, CancellationToken ct = default);

    Task PutAsync(string key, byte[] value, CancellationToken ct = default);

    Task PutAsync(string key, byte[] value, TimeSpan ttl, CancellationToken ct = default);

    Task DeleteAsync(string key, CancellationToken ct = default);

    Task BatchAsync(Action<IBatchBuilder> configure, CancellationToken ct = default);

    Task<IReadOnlyDictionary<string, byte[]>> MultiGetAsync(IReadOnlyList<string> keys, CancellationToken ct = default);

    Task<ScanResult> ScanPrefixAsync(string prefix, int limit = 100, string? cursor = null, CancellationToken ct = default);

    IAsyncEnumerable<KvEntry> ScanPrefixAllAsync(string prefix, CancellationToken ct = default);
}
