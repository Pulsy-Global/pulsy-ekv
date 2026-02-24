using Pulsy.EKV.Client.Models;
using Pulsy.EKV.Grpc;

namespace Pulsy.EKV.Client.Operations;

internal sealed class BatchBuilder : IBatchBuilder
{
    public List<BatchOp> Operations { get; } = [];

    public void Put(string key, byte[] value) =>
        Operations.Add(new BatchOp(Key: key, Value: value));

    public void Put(string key, byte[] value, TimeSpan ttl) =>
        Operations.Add(new BatchOp(Key: key, Value: value, TtlMs: (long)ttl.TotalMilliseconds));

    public void Delete(string key) =>
        Operations.Add(new BatchOp(Type: BatchOpType.Delete, Key: key));
}
