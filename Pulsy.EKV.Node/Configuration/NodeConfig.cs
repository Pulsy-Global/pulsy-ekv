namespace Pulsy.EKV.Node.Configuration;

public sealed class NodeConfig
{
    public string Id { get; set; } = Environment.MachineName;

    public required string DataPath { get; set; }

    public string GrpcEndpoint { get; set; } = "http://localhost:8080";

    public int ShutdownTimeoutSeconds { get; set; } = 120;
}
