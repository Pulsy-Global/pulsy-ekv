namespace Pulsy.EKV.Client.Configuration;

public sealed class EkvClientOptions
{
    public string Endpoint { get; set; } = "http://localhost:8080";

    public int MaxMessageBytes { get; set; } = 64 * 1024 * 1024;

    public int RetryMaxAttempts { get; set; } = 5;

    public int RetryInitialBackoffMs { get; set; } = 100;

    public int RetryMaxBackoffMs { get; set; } = 5000;

    public double RetryBackoffMultiplier { get; set; } = 2.0;
}
