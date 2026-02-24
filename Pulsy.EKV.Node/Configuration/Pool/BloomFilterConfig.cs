namespace Pulsy.EKV.Node.Configuration.Pool;

public sealed class BloomFilterConfig
{
    public int BitsPerKey { get; set; } = 10;

    public int MinFilterKeys { get; set; } = 1000;
}
