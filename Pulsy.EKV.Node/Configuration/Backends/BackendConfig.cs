namespace Pulsy.EKV.Node.Configuration.Backends;

public sealed class BackendConfig
{
    public BackendType Type { get; set; } = BackendType.Local;

    public string? Bucket { get; set; }

    public string? Region { get; set; }

    public string? Endpoint { get; set; }

    public string? AccessKeyId { get; set; }

    public string? SecretAccessKey { get; set; }

    public bool AllowHttp { get; set; }
}
