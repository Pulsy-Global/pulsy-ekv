namespace Pulsy.EKV.Node.Configuration;

public sealed class NatsConfig
{
    public string Url { get; set; } = "nats://localhost:4222";

    public int RequestTimeoutSeconds { get; set; } = 30;
}
