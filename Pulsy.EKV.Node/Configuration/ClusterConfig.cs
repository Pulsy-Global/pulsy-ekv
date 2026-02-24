namespace Pulsy.EKV.Node.Configuration;

public sealed class ClusterConfig
{
    public bool ClusterMode { get; set; }

    public int LeaseTtlSeconds { get; set; } = 30;

    public int LeaseRenewSeconds { get; set; } = 20;

    public int StatusTtlSeconds { get; set; } = 15;

    public int StatusIntervalSeconds { get; set; } = 10;

    public int LeaderTtlSeconds { get; set; } = 15;

    public int LeaderRenewSeconds { get; set; } = 10;

    public int ClusterPollSeconds { get; set; } = 5;

    public int DrainTimeoutSeconds { get; set; } = 60;
}
