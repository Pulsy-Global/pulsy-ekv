using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Diagnostics;

public sealed class SlateDbMetricsCollector : IHostedService, IDisposable
{
    public const string MeterName = "Pulsy.EKV.SlateDb";

    private readonly DatabasePool _pool;
    private readonly ILogger<SlateDbMetricsCollector> _logger;
    private readonly DiagnosticsConfig _diagnosticsConfig;
    private readonly Dictionary<string, long> _latest = new();
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
        RegisterGauge(meter, "slatedb.get_requests", "db/get_requests", "Total get requests");
        RegisterGauge(meter, "slatedb.write_ops", "db/write_ops", "Total write operations");
        RegisterGauge(meter, "slatedb.scan_requests", "db/scan_requests", "Total scan requests");
        RegisterGauge(meter, "slatedb.write_batch_count", "db/write_batch_count", "Total write batches");
        RegisterGauge(meter, "slatedb.flush_requests", "db/flush_requests", "Total flush requests");

        // Memory & SSTs
        RegisterGauge(meter, "slatedb.total_mem_size_bytes", "db/total_mem_size_bytes", "Total memory usage");
        RegisterGauge(meter, "slatedb.l0_sst_count", "db/l0_sst_count", "L0 SST file count");

        // WAL
        RegisterGauge(meter, "slatedb.wal_buffer_estimated_bytes", "db/wal_buffer_estimated_bytes", "WAL buffer size");
        RegisterGauge(meter, "slatedb.wal_buffer_flushes", "db/wal_buffer_flushes", "WAL buffer flushes");
        RegisterGauge(meter, "slatedb.immutable_memtable_flushes", "db/immutable_memtable_flushes", "Immutable memtable flushes");
        RegisterGauge(meter, "slatedb.backpressure_count", "db/backpressure_count", "Backpressure events");

        // Bloom filter
        RegisterGauge(meter, "slatedb.sst_filter_false_positives", "db/sst_filter_false_positives", "Bloom filter false positives");
        RegisterGauge(meter, "slatedb.sst_filter_positives", "db/sst_filter_positives", "Bloom filter positives");
        RegisterGauge(meter, "slatedb.sst_filter_negatives", "db/sst_filter_negatives", "Bloom filter negatives");

        // Block cache
        RegisterGauge(meter, "slatedb.cache_index_hit", "dbcache/index_hit", "Block cache index hits");
        RegisterGauge(meter, "slatedb.cache_index_miss", "dbcache/index_miss", "Block cache index misses");
        RegisterGauge(meter, "slatedb.cache_data_block_hit", "dbcache/data_block_hit", "Block cache data hits");
        RegisterGauge(meter, "slatedb.cache_data_block_miss", "dbcache/data_block_miss", "Block cache data misses");
        RegisterGauge(meter, "slatedb.cache_filter_hit", "dbcache/filter_hit", "Block cache filter hits");
        RegisterGauge(meter, "slatedb.cache_filter_miss", "dbcache/filter_miss", "Block cache filter misses");

        // Compactor
        RegisterGauge(meter, "slatedb.compactor_bytes_compacted", "compactor/bytes_compacted", "Total bytes compacted");
        RegisterGauge(meter, "slatedb.compactor_running", "compactor/running_compactions", "Running compactions");
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

    private void RegisterGauge(Meter meter, string instrumentName, string jsonKey, string description)
    {
        meter.CreateObservableGauge(
            instrumentName,
            () =>
            {
                lock (_lock)
                {
                    return _latest.GetValueOrDefault(jsonKey, 0);
                }
            },
            description: description);
    }

    private void Collect(object? state)
    {
        try
        {
            var entries = _pool.GetOpenEntries();
            var aggregated = new Dictionary<string, long>();

            foreach (var (name, store) in entries)
            {
                try
                {
                    var json = store.Metrics();
                    var metrics = JsonSerializer.Deserialize<Dictionary<string, long>>(json);
                    if (metrics == null)
                    {
                        continue;
                    }

                    foreach (var (key, value) in metrics)
                    {
                        aggregated[key] = aggregated.GetValueOrDefault(key) + value;
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
}
