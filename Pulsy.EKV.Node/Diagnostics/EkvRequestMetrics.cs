using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using Pulsy.EKV.Node.Configuration;

namespace Pulsy.EKV.Node.Diagnostics;

public sealed class EkvRequestMetrics
{
    public const string MeterName = "Pulsy.EKV.Requests";

    private readonly Histogram<double> _duration;
    private readonly Counter<long> _ops;
    private readonly Counter<long> _slowRequests;
    private readonly Counter<long> _scanItems;
    private readonly double _slowRequestThresholdSeconds;

    public EkvRequestMetrics(IMeterFactory meterFactory, IOptions<DiagnosticsConfig> diagnosticsConfig)
    {
        _slowRequestThresholdSeconds = diagnosticsConfig.Value.SlowRequestThresholdSeconds;

        var meter = meterFactory.Create(MeterName);

        // Latency
        _duration = meter.CreateHistogram<double>(
            "ekv.request.duration",
            unit: "s",
            description: "Request duration by method");

        _slowRequests = meter.CreateCounter<long>(
            "ekv.slow_requests",
            description: $"Requests exceeding {_slowRequestThresholdSeconds * 1000:F0}ms by method and namespace");

        // Throughput
        _ops = meter.CreateCounter<long>(
            "ekv.ops",
            description: "Total operations by method and namespace");

        _scanItems = meter.CreateCounter<long>(
            "ekv.scan_items",
            description: "Total items returned by scan operations");
    }

    public void RecordRequest(string method, string ns, double durationSec)
    {
        var methodTag = new KeyValuePair<string, object?>("method", method);
        var nsTag = new KeyValuePair<string, object?>("namespace", ns);

        _duration.Record(durationSec, methodTag);
        _ops.Add(1, methodTag, nsTag);

        if (durationSec > _slowRequestThresholdSeconds)
        {
            _slowRequests.Add(1, methodTag, nsTag);
        }
    }

    public void RecordScanItems(string ns, int count)
        => _scanItems.Add(count, new KeyValuePair<string, object?>("namespace", ns));
}
