using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Configuration.Backends;
using Pulsy.EKV.Node.Configuration.Pool;
using Pulsy.EKV.Node.Diagnostics;
using Pulsy.EKV.Node.Grpc;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node;

public static class EkvApp
{
    public static WebApplication Build(string[] args, Action<WebApplicationBuilder>? configure = null)
    {
        var builder = WebApplication.CreateBuilder(args);

        configure?.Invoke(builder);

        var nodeConfig = builder.Configuration.GetSection("Node").Get<NodeConfig>()!;

        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(nodeConfig.ShutdownTimeoutSeconds));
        builder.Services.Configure<NodeConfig>(builder.Configuration.GetSection("Node"));
        builder.Services.Configure<NatsConfig>(builder.Configuration.GetSection("Nats"));
        builder.Services.Configure<PoolConfig>(builder.Configuration.GetSection("Pool"));
        builder.Services.Configure<ClusterConfig>(builder.Configuration.GetSection("Cluster"));
        builder.Services.Configure<LimitsConfig>(builder.Configuration.GetSection("Limits"));
        builder.Services.Configure<DiagnosticsConfig>(builder.Configuration.GetSection("Diagnostics"));
        builder.Services.Configure<BackendsConfig>(builder.Configuration);

        builder.Services.AddSingleton<DatabasePool>();
        builder.Services.AddSingleton<EkvMetrics>();
        builder.Services.AddSingleton<EkvRequestMetrics>();
        builder.Services.AddHostedService<SlateDbMetricsCollector>();

        builder.Services.AddEkvCluster(builder.Configuration);

        var limitsConfig = builder.Configuration.GetSection("Limits").Get<LimitsConfig>() ?? new LimitsConfig();

        builder.Services.AddGrpc(options =>
        {
            options.MaxReceiveMessageSize = limitsConfig.MaxGrpcMessageBytes;
            options.MaxSendMessageSize = limitsConfig.MaxGrpcMessageBytes;
            options.Interceptors.Add<ExceptionInterceptor>();
        });

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(r => r
                .AddService("ekv", serviceInstanceId: nodeConfig.Id))
            .WithMetrics(m => m
                .AddAspNetCoreInstrumentation()
                .AddMeter(EkvMetrics.MeterName)
                .AddMeter(SlateDbMetricsCollector.MeterName)
                .AddMeter(EkvRequestMetrics.MeterName)
                .AddOtlpExporter());

        var app = builder.Build();

        app.Services.GetRequiredService<DatabasePool>()
            .SetMetrics(app.Services.GetRequiredService<EkvMetrics>());

        app.MapGrpcService<EkvStoreService>();
        app.MapGrpcService<EkvAdminService>();

        app.MapGet(
            "/health",
            (NamespaceCoordinator coordinator) =>
            coordinator.IsHealthy ? Results.Ok("healthy") : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));

        return app;
    }
}
