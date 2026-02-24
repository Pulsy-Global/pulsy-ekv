using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Pulsy.EKV.Client.Models;
using Pulsy.EKV.Client.Operations;
using Pulsy.EKV.Grpc;

namespace Pulsy.EKV.Client.Namespaces;

public sealed class EkvNamespace : IEkvNamespace
{
    private readonly EkvStore.EkvStoreClient _client;
    private readonly string _ns;

    public EkvNamespace(GrpcChannel channel, string ns)
    {
        _client = new EkvStore.EkvStoreClient(channel);
        _ns = ns;
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken ct = default)
    {
        var resp = await _client.GetAsync(
            new GetRequest { Namespace = _ns, Key = key },
            cancellationToken: ct);
        return resp.Found ? resp.Value.ToByteArray() : null;
    }

    public Task PutAsync(string key, byte[] value, CancellationToken ct = default) =>
        PutAsync(key, value, null, ct);

    public Task PutAsync(string key, byte[] value, TimeSpan ttl, CancellationToken ct = default) =>
        PutAsync(key, value, (long)ttl.TotalMilliseconds, ct);

    private async Task PutAsync(string key, byte[] value, long? ttlMs, CancellationToken ct)
    {
        var req = new PutRequest
        {
            Namespace = _ns,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(value)
        };
        if (ttlMs.HasValue)
        {
            req.TtlMs = ttlMs.Value;
        }

        await _client.PutAsync(req, cancellationToken: ct);
    }

    public async Task DeleteAsync(string key, CancellationToken ct = default)
    {
        await _client.DeleteAsync(
            new DeleteRequest { Namespace = _ns, Key = key },
            cancellationToken: ct);
    }

    public async Task BatchAsync(Action<IBatchBuilder> configure, CancellationToken ct = default)
    {
        var builder = new BatchBuilder();
        configure(builder);

        if (builder.Operations.Count == 0)
        {
            return;
        }

        var req = new BatchRequest { Namespace = _ns };
        foreach (var op in builder.Operations)
        {
            var batchOp = new BatchOperation
            {
                Type = op.Type,
                Key = op.Key,
            };
            if (op.Value != null)
            {
                batchOp.Value = UnsafeByteOperations.UnsafeWrap(op.Value);
            }

            if (op.TtlMs.HasValue)
            {
                batchOp.TtlMs = op.TtlMs.Value;
            }

            req.Ops.Add(batchOp);
        }

        await _client.BatchAsync(req, cancellationToken: ct);
    }

    public async Task<IReadOnlyDictionary<string, byte[]>> MultiGetAsync(
        IReadOnlyList<string> keys,
        CancellationToken ct = default)
    {
        var req = new MultiGetRequest { Namespace = _ns };
        req.Keys.AddRange(keys);

        var resp = await _client.MultiGetAsync(req, cancellationToken: ct);
        var dict = new Dictionary<string, byte[]>(resp.Entries.Count);
        foreach (var e in resp.Entries)
        {
            dict[e.Key] = e.Value.ToByteArray();
        }

        return dict;
    }

    public async Task<ScanResult> ScanPrefixAsync(
        string prefix,
        int limit = 100,
        string? cursor = null,
        CancellationToken ct = default)
    {
        var req = new ScanRequest { Namespace = _ns, Prefix = prefix, Limit = limit + 1 };
        if (cursor != null)
        {
            req.Cursor = cursor;
        }

        return await ReadScanPage(req, limit, ct);
    }

    public async IAsyncEnumerable<KvEntry> ScanPrefixAllAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var req = new ScanRequest { Namespace = _ns, Prefix = prefix, Limit = 0 };

        using var stream = _client.Scan(req, cancellationToken: ct);
        await foreach (var entry in stream.ResponseStream.ReadAllAsync(ct))
        {
            yield return new KvEntry { Key = entry.Key, Value = entry.Value.ToByteArray() };
        }
    }

    private async Task<ScanResult> ReadScanPage(ScanRequest req, int limit, CancellationToken ct)
    {
        using var stream = _client.Scan(req, cancellationToken: ct);
        var items = new List<KvEntry>();

        await foreach (var entry in stream.ResponseStream.ReadAllAsync(ct))
        {
            items.Add(new KvEntry { Key = entry.Key, Value = entry.Value.ToByteArray() });
            if (items.Count > limit)
            {
                break;
            }
        }

        var hasMore = items.Count > limit;
        if (hasMore)
        {
            items.RemoveAt(items.Count - 1);
        }

        return new ScanResult
        {
            Items = items,
            NextCursor = hasMore ? items[^1].Key : null,
            HasMore = hasMore,
        };
    }
}
