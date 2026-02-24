using Pulsy.SlateDB.Options;

namespace Pulsy.EKV.Node.Configuration.Pool;

public sealed class PoolConfig
{
    public int MaxOpen { get; set; } = 100;

    public int IdleTimeoutSeconds { get; set; } = 300;

    public int EvictionIntervalSeconds { get; set; } = 30;

    public int OpenRetryMaxAttempts { get; set; } = 3;

    public int OpenRetryBaseDelayMs { get; set; } = 500;

    public CompressionCodec? Compression { get; set; } = CompressionCodec.Zstd;

    public int L0MaxSsts { get; set; } = 8;

    public long L0SstSizeBytes { get; set; } = 32 * 1024 * 1024;

    public long MaxUnflushedBytes { get; set; } = 128 * 1024 * 1024;

    public bool WalEnabled { get; set; } = true;

    public bool AwaitDurable { get; set; }

    public int ManifestPollIntervalSeconds { get; set; } = 30;

    public int CompactorPollIntervalSeconds { get; set; } = 30;

    public CompactorConfig Compactor { get; set; } = new();

    public BloomFilterConfig BloomFilter { get; set; } = new();

    public DiskCacheConfig DiskCache { get; set; } = new();
}
