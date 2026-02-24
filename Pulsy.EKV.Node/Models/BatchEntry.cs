using Pulsy.EKV.Grpc;

namespace Pulsy.EKV.Node.Models;

public sealed record BatchEntry(
    BatchOpType Type,
    string Key,
    byte[]? Value = null,
    TimeSpan? Ttl = null);
