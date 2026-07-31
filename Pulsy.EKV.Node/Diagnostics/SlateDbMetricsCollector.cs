using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Storage.DatabasePool;
using Pulsy.SlateDB.Metrics;

namespace Pulsy.EKV.Node.Diagnostics;

public sealed class SlateDbMetricsCollector : IHostedService, IDisposable
{
    public const string MeterName = "Pulsy.EKV.SlateDb";

    private readonly DatabasePool _pool;
    private readonly ILogger<SlateDbMetricsCollector> _logger;
    private readonly DiagnosticsConfig _diagnosticsConfig;
    private readonly Dictionary<string, long> _latest = new();
    private readonly Dictionary<string, MetricSelector> _selectors = new();
    private readonly Lock _lock = new();
    private Timer? _timer;

    public SlateDbMetricsCollector(
        IMeterFactory meterFactory,
        DatabasePool pool,
        IOptions<DiagnosticsConfig> diagnosticsConfig,
        ILogger<SlateDbMetricsCollector> logger)
    {
        _pool = pool;
        _logger = logger;
        _diagnosticsConfig = diagnosticsConfig.Value;

        var meter = meterFactory.Create(MeterName);

        // Operations
        RegisterGauge(
            meter,
            "slatedb.get_requests",
            "slatedb.db.request_count",
            "Total get requests",
            new SlateDbMetricLabel("op", "get"));
        RegisterGauge(meter, "slatedb.write_ops", "slatedb.db.write_ops", "Total write operations");
        RegisterGauge(
            meter,
            "slatedb.scan_requests",
            "slatedb.db.request_count",
            "Total scan requests",
            new SlateDbMetricLabel("op", "scan"));
        RegisterGauge(meter, "slatedb.write_batch_count", "slatedb.db.write_batch_count", "Total write batches");
        RegisterGauge(
            meter,
            "slatedb.flush_requests",
            "slatedb.db.request_count",
            "Total flush requests",
            new SlateDbMetricLabel("op", "flush"));

        // Memory & SSTs
        RegisterGauge(meter, "slatedb.total_mem_size_bytes", "slatedb.db.total_mem_size_bytes", "Total memory usage");
        RegisterGauge(meter, "slatedb.l0_sst_count", "slatedb.db.l0_sst_count", "L0 SST file count");

        // WAL
        RegisterGauge(meter, "slatedb.wal_buffer_estimated_bytes", "slatedb.wal.wal_buffer_estimated_bytes", "WAL buffer size");
        RegisterGauge(meter, "slatedb.wal_buffer_flushes", "slatedb.wal.wal_buffer_flushes", "WAL buffer flushes");
        RegisterGauge(meter, "slatedb.immutable_memtable_flushes", "slatedb.db.immutable_memtable_flushes", "Immutable memtable flushes");
        RegisterGauge(meter, "slatedb.backpressure_count", "slatedb.db.backpressure_count", "Backpressure events");

        // Bloom filter
        RegisterGauge(meter, "slatedb.sst_filter_false_positives", "slatedb.db.sst_filter_false_positive_count", "Bloom filter false positives");
        RegisterGauge(meter, "slatedb.sst_filter_positives", "slatedb.db.sst_filter_positive_count", "Bloom filter positives");
        RegisterGauge(meter, "slatedb.sst_filter_negatives", "slatedb.db.sst_filter_negative_count", "Bloom filter negatives");

        // Block cache
        RegisterCacheGauge(meter, "slatedb.cache_index_hit", "index", "hit", "Block cache index hits");
        RegisterCacheGauge(meter, "slatedb.cache_index_miss", "index", "miss", "Block cache index misses");
        RegisterCacheGauge(meter, "slatedb.cache_data_block_hit", "data_block", "hit", "Block cache data hits");
        RegisterCacheGauge(meter, "slatedb.cache_data_block_miss", "data_block", "miss", "Block cache data misses");
        RegisterCacheGauge(meter, "slatedb.cache_filter_hit", "filter", "hit", "Block cache filter hits");
        RegisterCacheGauge(meter, "slatedb.cache_filter_miss", "filter", "miss", "Block cache filter misses");

        // Compactor
        RegisterGauge(meter, "slatedb.compactor_bytes_compacted", "slatedb.compactor.bytes_compacted", "Total bytes compacted");
        RegisterGauge(meter, "slatedb.compactor_running", "slatedb.compactor.running_compactions", "Running compactions");
    }

    public Task StartAsync(CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(_diagnosticsConfig.MetricsCollectionIntervalSeconds);
        _timer = new Timer(Collect, null, interval, interval);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken ct)
    {
        _timer?.Change(Timeout.Infinite, Timeout.Infinite);
        return Task.CompletedTask;
    }

    public void Dispose() => _timer?.Dispose();

    private static bool TryGetValue(SlateDbMetricValue metric, out long value)
    {
        switch (metric)
        {
            case SlateDbCounterMetricValue counter:
                value = counter.Value > long.MaxValue ? long.MaxValue : (long)counter.Value;
                return true;
            case SlateDbGaugeMetricValue gauge:
                value = gauge.Value;
                return true;
            case SlateDbUpDownCounterMetricValue counter:
                value = counter.Value;
                return true;
            default:
                value = 0;
                return false;
        }
    }

    private static long AddSaturating(long left, long right)
    {
        if (right > 0 && left > long.MaxValue - right)
        {
            return long.MaxValue;
        }

        if (right < 0 && left < long.MinValue - right)
        {
            return long.MinValue;
        }

        return left + right;
    }

    private void RegisterGauge(
        Meter meter,
        string instrumentName,
        string metricName,
        string description,
        params SlateDbMetricLabel[] labels)
    {
        _selectors[instrumentName] = new MetricSelector(metricName, labels);
        meter.CreateObservableGauge(
            instrumentName,
            () =>
            {
                lock (_lock)
                {
                    return _latest.GetValueOrDefault(instrumentName, 0);
                }
            },
            description: description);
    }

    private void RegisterCacheGauge(
        Meter meter,
        string instrumentName,
        string entryKind,
        string result,
        string description)
        => RegisterGauge(
            meter,
            instrumentName,
            "slatedb.db_cache.access_count",
            description,
            new SlateDbMetricLabel("entry_kind", entryKind),
            new SlateDbMetricLabel("result", result));

    private void Collect(object? state)
    {
        try
        {
            var entries = _pool.ListOpenStores();
            var aggregated = new Dictionary<string, long>();

            foreach (var (name, store) in entries)
            {
                try
                {
                    foreach (var metric in store.Metrics())
                    {
                        foreach (var (instrumentName, selector) in _selectors)
                        {
                            if (selector.Matches(metric) && TryGetValue(metric.Value, out var value))
                            {
                                aggregated[instrumentName] = AddSaturating(
                                    aggregated.GetValueOrDefault(instrumentName),
                                    value);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Failed to collect metrics for namespace {Namespace}", name);
                }
            }

            lock (_lock)
            {
                _latest.Clear();
                foreach (var (key, value) in aggregated)
                {
                    _latest[key] = value;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error collecting SlateDB metrics");
        }
    }

    private sealed record MetricSelector(string Name, IReadOnlyList<SlateDbMetricLabel> Labels)
    {
        public bool Matches(SlateDbMetric metric)
            => metric.Name == Name && Labels.All(metric.Labels.Contains);
    }
}
