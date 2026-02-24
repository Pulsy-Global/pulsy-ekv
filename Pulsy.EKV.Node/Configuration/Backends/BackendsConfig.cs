namespace Pulsy.EKV.Node.Configuration.Backends;

public sealed class BackendsConfig
{
    public Dictionary<string, BackendConfig> Backends { get; set; } = new()
    {
        ["default"] = new BackendConfig { Type = BackendType.Local }
    };
}
