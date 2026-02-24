using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Extensions.Microsoft.DependencyInjection;
using Pulsy.EKV.Node.Cluster.Coordination;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Cluster.Placement;
using Pulsy.EKV.Node.Cluster.Registry;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Cluster.Routing;
using Pulsy.EKV.Node.Engine;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEkvCluster(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var clusterConfig = configuration.GetSection("Cluster").Get<ClusterConfig>()!;

        if (clusterConfig.ClusterMode)
        {
            services.AddNatsClient(nats => nats.ConfigureOptions(opts =>
                opts.Configure<IOptions<NatsConfig>>((o, natsConfig) => o.Opts = o.Opts with
                {
                    Url = natsConfig.Value.Url,
                    RequestTimeout = TimeSpan.FromSeconds(natsConfig.Value.RequestTimeoutSeconds),
                })));

            services.AddSingleton(sp =>
                new NatsKVContext(new NatsJSContext(sp.GetRequiredService<INatsConnection>())));

            services.AddSingleton<INamespaceRegistry, NatsNamespaceRegistry>();
            services.AddSingleton<ILeaseManager, NatsLeaseManager>();
            services.AddSingleton<IPlacementStrategy, NatsPlacementStrategy>();
            services.AddSingleton<NodeRouter>();
            services.AddHostedService(sp => sp.GetRequiredService<NodeRouter>());
            services.AddSingleton<LeaderElection>();
            services.AddSingleton<NamespaceRedistributor>();
            services.AddHostedService<NodeStatusPublisher>();
            services.AddHostedService<ClusterCoordinator>();
        }
        else
        {
            services.AddSingleton<INamespaceRegistry, InMemoryNamespaceRegistry>();
        }

        services.AddSingleton<NamespaceCoordinator>();
        services.AddSingleton<EkvEngine>();
        services.AddHostedService(sp => sp.GetRequiredService<DatabasePool>());
        services.AddHostedService(sp => sp.GetRequiredService<NamespaceCoordinator>());

        return services;
    }
}
