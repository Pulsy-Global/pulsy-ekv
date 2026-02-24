using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Pulsy.EKV.Node;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Cluster.Registry;
using Pulsy.EKV.Node.Models;

namespace Pulsy.EKV.IntegrationTests.Infrastructure;

public sealed class EkvNodeInstance : IAsyncDisposable
{
    private readonly WebApplication _app;

    private EkvNodeInstance(WebApplication app, int port, string nodeId)
    {
        _app = app;
        Port = port;
        NodeId = nodeId;
    }

    public int Port { get; }

    public string NodeId { get; }

    public IServiceProvider Services => _app.Services;

    public static EkvNodeInstance Create(string nodeId, int port, string dataPath, string natsUrl)
    {
        var overrides = new Dictionary<string, string?>
        {
            ["Node:Id"] = nodeId,
            ["Node:DataPath"] = dataPath,
            ["Node:GrpcEndpoint"] = $"http://localhost:{port}",
            ["Node:ShutdownTimeoutSeconds"] = "5",

            ["Kestrel:Endpoints:Grpc:Url"] = $"http://0.0.0.0:{port}",
            ["Kestrel:Endpoints:Grpc:Protocols"] = "Http2",

            ["Cluster:ClusterMode"] = "true",
            ["Cluster:LeaseTtlSeconds"] = "5",
            ["Cluster:LeaseRenewSeconds"] = "3",
            ["Cluster:StatusTtlSeconds"] = "5",
            ["Cluster:StatusIntervalSeconds"] = "2",
            ["Cluster:LeaderTtlSeconds"] = "5",
            ["Cluster:LeaderRenewSeconds"] = "3",
            ["Cluster:ClusterPollSeconds"] = "3",
            ["Cluster:DrainTimeoutSeconds"] = "5",

            // NATS
            ["Nats:Url"] = natsUrl,

            // Backends (must provide a default backend)
            ["Backends:default:Type"] = "Local",

            ["Pool:MaxOpen"] = "100",
            ["Pool:AwaitDurable"] = "true",
            ["Pool:WalEnabled"] = "true",
            ["Pool:DiskCache:Enabled"] = "false",
            ["Pool:Compression"] = "lz4",

            ["Limits:MaxValueBytes"] = "409600",
            ["Limits:MaxKeyBytes"] = "512",

            ["Logging:LogLevel:Default"] = "Warning",
            ["Logging:LogLevel:Microsoft.AspNetCore"] = "Warning",
        };

        var app = EkvApp.Build([], builder =>
        {
            builder.Configuration.AddInMemoryCollection(overrides);
        });

        return new EkvNodeInstance(app, port, nodeId);
    }

    public Task StartAsync(CancellationToken ct = default) => _app.StartAsync(ct);

    public Task StopAsync(CancellationToken ct = default) => _app.StopAsync(ct);

    public async Task CreateNamespaceAsync(string name, CancellationToken ct = default)
    {
        var registry = Services.GetRequiredService<INamespaceRegistry>();
        var coordinator = Services.GetRequiredService<NamespaceCoordinator>();

        var config = new NamespaceConfig { Name = name, Backend = "default" };
        await registry.CreateAsync(config, ct);
        await coordinator.AcceptAssignmentAsync(name, config.Backend, ct);
    }

    public async Task ClaimNamespaceAsync(string name, CancellationToken ct = default)
    {
        var coordinator = Services.GetRequiredService<NamespaceCoordinator>();
        await coordinator.AcceptAssignmentAsync(name, "default", ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.DisposeAsync();
    }
}
