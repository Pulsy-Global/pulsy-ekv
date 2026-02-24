namespace Pulsy.EKV.Node.Configuration.Pool;

public sealed class CompactorConfig
{
    public int MaxConcurrentCompactions { get; set; } = 2;

    public long MaxSstSizeBytes { get; set; } = 256 * 1024 * 1024;
}
