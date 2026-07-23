using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Extensions.Microsoft.DependencyInjection;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Cluster.Registry;
using Pulsy.EKV.Node.Cluster.Routing;
using Pulsy.EKV.Node.Cluster.Status;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Engine;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEkvCluster(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var clusterConfig = configuration.GetSection("Cluster").Get<ClusterConfig>()!;

        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<NatsHealthState>();

        if (clusterConfig.ClusterMode)
        {
            services.AddNatsClient(nats => nats
                .ConfigureOptions(opts =>
                    opts.Configure<IOptions<NatsConfig>>((o, natsConfig) => o.Opts = o.Opts with
                    {
                        Url = natsConfig.Value.Url,
                        RequestTimeout = TimeSpan.FromSeconds(natsConfig.Value.RequestTimeoutSeconds),
                    }))

                // A full KV watcher must never block the shared socket reader (nats.net#1181).
                .WithSubPendingChannelFullMode(BoundedChannelFullMode.DropNewest));

            services.AddSingleton(sp =>
                new NatsKVContext(new NatsJSContext(sp.GetRequiredService<INatsConnection>())));

            services.AddSingleton<INamespaceRegistry, NatsNamespaceRegistry>();
            services.AddSingleton<ILeaseManager, NatsLeaseManager>();
            services.AddSingleton<NodeRouter>();
            services.AddHostedService(sp => sp.GetRequiredService<NodeRouter>());
            services.AddHostedService<NodeStatusPublisher>();
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
