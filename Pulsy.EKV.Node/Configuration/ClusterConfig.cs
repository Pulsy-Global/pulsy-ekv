namespace Pulsy.EKV.Node.Configuration;

public sealed class ClusterConfig
{
    public bool ClusterMode { get; set; }

    public string? DefaultBackend { get; set; }

    public int LeaseTtlSeconds { get; set; } = 30;

    public int LeaseRenewSeconds { get; set; } = 20;

    public int StatusTtlSeconds { get; set; } = 15;

    public int StatusIntervalSeconds { get; set; } = 10;

    public int NatsHealthTimeoutSeconds { get; set; } = 120;
}
