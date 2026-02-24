namespace Pulsy.EKV.Node.Configuration;

public sealed class DiagnosticsConfig
{
    public int MetricsCollectionIntervalSeconds { get; set; } = 10;

    public double SlowRequestThresholdSeconds { get; set; } = 0.5;
}
