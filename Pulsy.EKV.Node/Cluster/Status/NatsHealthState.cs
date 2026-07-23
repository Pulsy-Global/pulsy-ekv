using Microsoft.Extensions.Options;
using Pulsy.EKV.Node.Configuration;

namespace Pulsy.EKV.Node.Cluster.Status;

public sealed class NatsHealthState
{
    private readonly bool _enabled;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _timeout;
    private long _lastSuccessTimestamp;

    public NatsHealthState(
        IOptions<ClusterConfig> clusterConfig,
        TimeProvider timeProvider)
    {
        var config = clusterConfig.Value;
        _enabled = config.ClusterMode;
        _timeProvider = timeProvider;
        _timeout = TimeSpan.FromSeconds(config.NatsHealthTimeoutSeconds);
        _lastSuccessTimestamp = timeProvider.GetTimestamp();
    }

    public bool IsHealthy =>
        !_enabled
        || _timeProvider.GetElapsedTime(Interlocked.Read(ref _lastSuccessTimestamp)) <= _timeout;

    public void ReportSuccess()
    {
        Interlocked.Exchange(ref _lastSuccessTimestamp, _timeProvider.GetTimestamp());
    }
}
