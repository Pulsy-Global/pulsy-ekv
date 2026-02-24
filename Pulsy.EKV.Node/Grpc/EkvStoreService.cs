using System.Diagnostics;
using Google.Protobuf;
using Grpc.Core;
using Pulsy.EKV.Grpc;
using Pulsy.EKV.Node.Cluster.Routing;
using Pulsy.EKV.Node.Diagnostics;
using Pulsy.EKV.Node.Engine;
using Pulsy.EKV.Node.Models;

namespace Pulsy.EKV.Node.Grpc;

public sealed class EkvStoreService : EkvStore.EkvStoreBase
{
    private readonly EkvEngine _engine;
    private readonly NodeRouter? _router;
    private readonly EkvRequestMetrics _requestMetrics;

    public EkvStoreService(EkvEngine engine, EkvRequestMetrics requestMetrics, NodeRouter? router = null)
    {
        _engine = engine;
        _router = router;
        _requestMetrics = requestMetrics;
    }

    public override async Task<GetResponse> Get(GetRequest request, ServerCallContext context)
    {
        ValidateNamespace(request.Namespace);
        var sw = Stopwatch.StartNew();
        try
        {
            var (handled, value) = await _engine.GetAsync(request.Namespace, request.Key, context.CancellationToken);
            if (handled)
            {
                return value != null
                    ? new GetResponse { Found = true, Value = ByteString.CopyFrom(value) }
                    : new GetResponse { Found = false };
            }

            var client = await GetForwardingClient(request.Namespace, context.CancellationToken);
            return await client.GetAsync(request, cancellationToken: context.CancellationToken);
        }
        finally
        {
            _requestMetrics.RecordRequest("Get", request.Namespace, sw.Elapsed.TotalSeconds);
        }
    }

    public override async Task<PutResponse> Put(PutRequest request, ServerCallContext context)
    {
        ValidateNamespace(request.Namespace);
        var sw = Stopwatch.StartNew();
        try
        {
            var ttl = ParseTtl(request.HasTtlMs, request.TtlMs);
            if (await _engine.PutAsync(request.Namespace, request.Key, request.Value.ToByteArray(), ttl, context.CancellationToken))
            {
                return new PutResponse();
            }

            var client = await GetForwardingClient(request.Namespace, context.CancellationToken);
            return await client.PutAsync(request, cancellationToken: context.CancellationToken);
        }
        finally
        {
            _requestMetrics.RecordRequest("Put", request.Namespace, sw.Elapsed.TotalSeconds);
        }
    }

    public override async Task<DeleteResponse> Delete(DeleteRequest request, ServerCallContext context)
    {
        ValidateNamespace(request.Namespace);
        var sw = Stopwatch.StartNew();
        try
        {
            if (await _engine.DeleteAsync(request.Namespace, request.Key, context.CancellationToken))
            {
                return new DeleteResponse();
            }

            var client = await GetForwardingClient(request.Namespace, context.CancellationToken);
            return await client.DeleteAsync(request, cancellationToken: context.CancellationToken);
        }
        finally
        {
            _requestMetrics.RecordRequest("Delete", request.Namespace, sw.Elapsed.TotalSeconds);
        }
    }

    public override async Task<BatchResponse> Batch(BatchRequest request, ServerCallContext context)
    {
        ValidateNamespace(request.Namespace);
        var sw = Stopwatch.StartNew();
        try
        {
            var entries = new List<BatchEntry>(request.Ops.Count);
            foreach (var op in request.Ops)
            {
                entries.Add(
                    op.Type == BatchOpType.Delete
                    ? new BatchEntry(BatchOpType.Delete, op.Key)
                    : new BatchEntry(BatchOpType.Put, op.Key, op.Value.ToByteArray(), ParseTtl(op.HasTtlMs, op.TtlMs)));
            }

            if (await _engine.WriteBatchAsync(request.Namespace, entries, context.CancellationToken))
            {
                return new BatchResponse();
            }

            var client = await GetForwardingClient(request.Namespace, context.CancellationToken);
            return await client.BatchAsync(request, cancellationToken: context.CancellationToken);
        }
        finally
        {
            _requestMetrics.RecordRequest("Batch", request.Namespace, sw.Elapsed.TotalSeconds);
        }
    }

    public override async Task<MultiGetResponse> MultiGet(MultiGetRequest request, ServerCallContext context)
    {
        ValidateNamespace(request.Namespace);
        var sw = Stopwatch.StartNew();
        try
        {
            var (handled, results) = await _engine.MultiGetAsync(
                request.Namespace,
                request.Keys,
                context.CancellationToken);

            if (handled)
            {
                var response = new MultiGetResponse();
                foreach (var kv in results!)
                {
                    response.Entries.Add(new MultiGetEntry { Key = kv.Key, Value = ByteString.CopyFrom(kv.Value) });
                }

                return response;
            }

            var client = await GetForwardingClient(request.Namespace, context.CancellationToken);
            return await client.MultiGetAsync(request, cancellationToken: context.CancellationToken);
        }
        finally
        {
            _requestMetrics.RecordRequest("MultiGet", request.Namespace, sw.Elapsed.TotalSeconds);
        }
    }

    public override async Task Scan(
        ScanRequest request,
        IServerStreamWriter<ScanEntry> responseStream,
        ServerCallContext context)
    {
        ValidateNamespace(request.Namespace);

        if (request.Limit < 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "limit must be non-negative"));
        }

        var sw = Stopwatch.StartNew();
        var itemCount = 0;
        try
        {
            var (handled, items) = await _engine.ScanAsync(
                request.Namespace,
                request.HasPrefix ? request.Prefix : null,
                request.Limit,
                request.HasCursor ? request.Cursor : null,
                context.CancellationToken);

            if (handled)
            {
                await foreach (var kv in items.WithCancellation(context.CancellationToken))
                {
                    await responseStream.WriteAsync(
                        new ScanEntry
                        {
                            Key = kv.Key,
                            Value = ByteString.CopyFrom(kv.Value)
                        },
                        context.CancellationToken);

                    itemCount++;
                }

                return;
            }

            var client = await GetForwardingClient(request.Namespace, context.CancellationToken);
            using var remoteStream = client.Scan(request, cancellationToken: context.CancellationToken);

            await foreach (var entry in remoteStream.ResponseStream.ReadAllAsync(context.CancellationToken))
            {
                await responseStream.WriteAsync(entry, context.CancellationToken);
                itemCount++;
            }
        }
        finally
        {
            _requestMetrics.RecordRequest("Scan", request.Namespace, sw.Elapsed.TotalSeconds);
            _requestMetrics.RecordScanItems(request.Namespace, itemCount);
        }
    }

    private static void ValidateNamespace(string namespaceName)
    {
        if (string.IsNullOrWhiteSpace(namespaceName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "namespace is required"));
        }
    }

    private static TimeSpan? ParseTtl(bool hasTtl, long ttlMs)
        => hasTtl ? TimeSpan.FromMilliseconds(ttlMs) : null;

    private async Task<EkvStore.EkvStoreClient> GetForwardingClient(
        string namespaceName,
        CancellationToken ct)
    {
        if (_router == null)
        {
            throw new RpcException(
                new Status(
                    StatusCode.NotFound,
                    $"namespace '{namespaceName}' not found"));
        }

        var client = await _router.GetForwardingClientAsync(namespaceName, ct);
        if (client == null)
        {
            throw new RpcException(
                new Status(
                    StatusCode.Unavailable,
                    $"namespace '{namespaceName}' has no owner"));
        }

        return client;
    }
}
