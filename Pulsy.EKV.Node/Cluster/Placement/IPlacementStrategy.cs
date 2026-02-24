namespace Pulsy.EKV.Node.Cluster.Placement;

public interface IPlacementStrategy
{
    Task InitAsync(CancellationToken ct = default);

    Task<string?> SelectNodeAsync(CancellationToken ct = default);
}
