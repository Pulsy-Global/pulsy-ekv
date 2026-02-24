namespace Pulsy.EKV.Node.Configuration;

public sealed class LimitsConfig
{
    public int MaxValueBytes { get; set; } = 512 * 1024;

    public int MaxKeyBytes { get; set; } = 512;

    public int MaxGrpcMessageBytes { get; set; } = 64 * 1024 * 1024;
}
