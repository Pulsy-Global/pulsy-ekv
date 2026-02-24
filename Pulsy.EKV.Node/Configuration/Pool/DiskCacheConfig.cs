namespace Pulsy.EKV.Node.Configuration.Pool;

public sealed class DiskCacheConfig
{
    public bool Enabled { get; set; }

    public string RootFolder { get; set; } = "data/cache";

    public int? MaxSizeMb { get; set; }

    public int TotalDiskBudgetMb { get; set; }

    public int FallbackSizeMb { get; set; } = 512;

    public bool CachePuts { get; set; } = true;

    public bool PreloadL0 { get; set; } = true;
}
