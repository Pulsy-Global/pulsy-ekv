using System.Text;
using Microsoft.Extensions.Options;
using Pulsy.EKV.Grpc;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Configuration.Pool;
using Pulsy.EKV.Node.Models;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Engine;

public sealed class EkvEngine
{
    private readonly NamespaceCoordinator _coordinator;
    private readonly LimitsConfig _limits;
    private readonly bool _awaitDurable;

    public EkvEngine(NamespaceCoordinator coordinator, IOptions<LimitsConfig> limits, IOptions<PoolConfig> poolConfig)
    {
        _coordinator = coordinator;
        _limits = limits.Value;
        _awaitDurable = poolConfig.Value.AwaitDurable;
    }

    public async Task<(bool Handled, byte[]? Value)> GetAsync(
        string namespaceName,
        string key,
        CancellationToken ct = default)
    {
        using var handle = await _coordinator.GetStoreAsync(namespaceName, ct);
        if (handle == null)
        {
            return (false, null);
        }

        ValidateKeyLength(key);
        var value = await Task.Run(() => handle.Store.Get(key), ct);

        return (true, value);
    }

    public async Task<bool> PutAsync(
        string namespaceName,
        string key,
        byte[] value,
        TimeSpan? ttl = null,
        CancellationToken ct = default)
    {
        using var handle = await _coordinator.GetStoreAsync(namespaceName, ct);
        if (handle == null)
        {
            return false;
        }

        ValidateKeyLength(key);
        ValidateValueSize(value);

        if (ttl.HasValue && ttl.Value <= TimeSpan.Zero)
        {
            throw new ArgumentException("TTL must be positive");
        }

        await Task.Run(() => handle.Store.Put(key, value, ttl, _awaitDurable), ct);

        return true;
    }

    public async Task<bool> DeleteAsync(
        string namespaceName,
        string key,
        CancellationToken ct = default)
    {
        using var handle = await _coordinator.GetStoreAsync(namespaceName, ct);
        if (handle == null)
        {
            return false;
        }

        ValidateKeyLength(key);
        await Task.Run(() => handle.Store.Delete(key, _awaitDurable), ct);

        return true;
    }

    public async Task<bool> WriteBatchAsync(
        string namespaceName,
        IReadOnlyList<BatchEntry> entries,
        CancellationToken ct = default)
    {
        using var handle = await _coordinator.GetStoreAsync(namespaceName, ct);
        if (handle == null)
        {
            return false;
        }

        foreach (var entry in entries)
        {
            ValidateKeyLength(entry.Key);
            if (entry is { Type: BatchOpType.Put, Value: not null })
            {
                ValidateValueSize(entry.Value);
            }
        }

        await Task.Run(() => handle.Store.WriteBatch(entries, _awaitDurable), ct);

        return true;
    }

    public async Task<(bool Handled, List<KeyValuePair<string, byte[]>>? Results)> MultiGetAsync(
        string namespaceName,
        IReadOnlyList<string> keys,
        CancellationToken ct = default)
    {
        using var handle = await _coordinator.GetStoreAsync(namespaceName, ct);
        if (handle == null)
        {
            return (false, null);
        }

        foreach (var key in keys)
        {
            ValidateKeyLength(key);
        }

        var results = await Task.Run(() => handle.Store.MultiGet(keys), ct);

        return (true, results);
    }

    public async Task<(bool Handled, IAsyncEnumerable<KeyValuePair<string, byte[]>> Items)> ScanAsync(
        string namespaceName,
        string? prefix,
        int limit,
        string? cursor = null,
        CancellationToken ct = default)
    {
        var handle = await _coordinator.GetStoreAsync(namespaceName, ct);
        if (handle == null)
        {
            return (false, EmptyAsyncEnumerable<KeyValuePair<string, byte[]>>.Instance);
        }

        return (true, ScanIterator(handle, prefix, limit, cursor, ct));
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanIterator(
        StoreHandle handle,
        string? prefix,
        int limit,
        string? cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        using (handle)
        {
            var iterator = handle.Store.CreatePrefixIterator(prefix ?? "");
            try
            {
                if (cursor != null)
                {
                    iterator.Seek(cursor);

                    var first = iterator.Next();
                    if (first == null)
                    {
                        yield break;
                    }

                    if (first.Value.Key != cursor)
                    {
                        yield return first.Value;
                    }
                }

                var count = 0;
                while (limit == 0 || count < limit)
                {
                    ct.ThrowIfCancellationRequested();

                    var kv = iterator.Next();
                    if (kv == null)
                    {
                        break;
                    }

                    yield return kv.Value;
                    count++;
                }
            }
            finally
            {
                iterator.Dispose();
            }
        }
    }

    private void ValidateKeyLength(string key)
    {
        if (Encoding.UTF8.GetByteCount(key) > _limits.MaxKeyBytes)
        {
            throw new ArgumentException("Key exceeds limit size");
        }
    }

    private void ValidateValueSize(byte[] value)
    {
        if (value.Length > _limits.MaxValueBytes)
        {
            throw new ArgumentException("Value exceeds limit size");
        }
    }
}

internal static class EmptyAsyncEnumerable<T>
{
    public static readonly IAsyncEnumerable<T> Instance = Create();

    private static async IAsyncEnumerable<T> Create()
    {
        await Task.CompletedTask;
        yield break;
    }
}
