using Pulsy.EKV.Grpc;

namespace Pulsy.EKV.Client.Models;

public sealed record BatchOp(
    string Key,
    BatchOpType Type = BatchOpType.Put,
    byte[]? Value = null,
    long? TtlMs = null);
