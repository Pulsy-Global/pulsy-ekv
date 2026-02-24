namespace Pulsy.EKV.Node.Cluster;

internal static class NatsSubjects
{
    public const string ClusterDrain = "ekv.cluster.drain";
    public const string AssignPrefix = "ekv.assign";

    public static string AssignNode(string nodeId) => $"{AssignPrefix}.{nodeId}";
}
