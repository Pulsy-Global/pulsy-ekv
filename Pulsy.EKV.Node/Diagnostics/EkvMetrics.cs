using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Storage;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Diagnostics;

public sealed class EkvMetrics
{
    public const string MeterName = "Pulsy.EKV.Node";

    private readonly Counter<long> _poolEvictions;
    private readonly Counter<long> _leaseAcquired;
    private readonly Counter<long> _leaseRenewed;
    private readonly Counter<long> _leaseLost;
    private readonly Counter<long> _leaseReleased;

    public EkvMetrics(IMeterFactory meterFactory, DatabasePool pool, IOptions<NodeConfig> nodeConfig, ILeaseManager? leaseManager = null)
    {
        var meter = meterFactory.Create(MeterName);
        var dataPath = nodeConfig.Value.DataPath;

        // Pool
        meter.CreateObservableGauge(
            "ekv.pool.open",
            () => pool.OpenCount,
            description: "Number of currently open SlateDB instances");

        _poolEvictions = meter.CreateCounter<long>(
            "ekv.pool.evictions",
            description: "Total pool evictions (idle + LRU)");

        // Disk
        meter.CreateObservableGauge(
            "ekv.disk.free_bytes",
            () => GetDiskFreeBytes(dataPath),
            unit: "By",
            description: "Available disk space on the data volume");

        // Leases
        _leaseAcquired = meter.CreateCounter<long>(
            "ekv.lease.acquired",
            description: "Total namespace leases acquired");

        _leaseRenewed = meter.CreateCounter<long>(
            "ekv.lease.renewed",
            description: "Total namespace lease renewals");

        _leaseLost = meter.CreateCounter<long>(
            "ekv.lease.lost",
            description: "Total namespace leases lost");

        _leaseReleased = meter.CreateCounter<long>(
            "ekv.lease.released",
            description: "Total namespace leases released");

        // Namespaces
        meter.CreateObservableGauge(
            "ekv.namespaces.owned",
            () => leaseManager?.OwnedNamespaces.Count ?? pool.OpenCount,
            description: "Number of namespaces currently owned by this node");
    }

    public void RecordEviction() => _poolEvictions.Add(1);

    public void RecordLeaseAcquired() => _leaseAcquired.Add(1);

    public void RecordLeaseRenewed() => _leaseRenewed.Add(1);

    public void RecordLeaseLost() => _leaseLost.Add(1);

    public void RecordLeaseReleased() => _leaseReleased.Add(1);

    private static long GetDiskFreeBytes(string dataPath) => DiskUtil.GetFreeBytes(dataPath);
}
