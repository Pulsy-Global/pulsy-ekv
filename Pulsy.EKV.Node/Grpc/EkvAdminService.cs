using Grpc.Core;
using Microsoft.Extensions.Options;
using Pulsy.EKV.Grpc;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Cluster.Registry;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Configuration.Backends;
using Pulsy.EKV.Node.Models;
using Pulsy.EKV.Node.Storage.DatabasePool;

namespace Pulsy.EKV.Node.Grpc;

public sealed class EkvAdminService : EkvAdmin.EkvAdminBase
{
    private readonly NamespaceCoordinator _coordinator;
    private readonly INamespaceRegistry _registry;
    private readonly DatabasePool _pool;
    private readonly BackendsConfig _backends;
    private readonly ClusterConfig _clusterConfig;
    private readonly ILogger<EkvAdminService> _logger;

    public EkvAdminService(
        NamespaceCoordinator coordinator,
        INamespaceRegistry registry,
        DatabasePool pool,
        IOptions<BackendsConfig> backends,
        IOptions<ClusterConfig> clusterConfig,
        ILogger<EkvAdminService> logger)
    {
        _coordinator = coordinator;
        _registry = registry;
        _pool = pool;
        _backends = backends.Value;
        _clusterConfig = clusterConfig.Value;
        _logger = logger;
    }

    public override async Task<CreateNamespaceResponse> CreateNamespace(
        CreateNamespaceRequest request,
        ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "name is required"));
        }

        ValidateNamespace(request.Name);
        var backend = ResolveBackend(request.Backend);

        var ns = request.Name;
        var ct = context.CancellationToken;

        var existing = await _registry.GetAsync(ns, ct);
        if (existing != null)
        {
            return new CreateNamespaceResponse();
        }

        var config = new NamespaceConfig { Name = ns, Backend = backend };
        await _registry.CreateAsync(config, ct);

        return new CreateNamespaceResponse();
    }

    public override async Task<GetNamespaceResponse> GetNamespace(
        GetNamespaceRequest request,
        ServerCallContext context)
    {
        ValidateNamespace(request.Name);
        var config = await _registry.GetAsync(request.Name, context.CancellationToken);
        if (config == null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"namespace '{request.Name}' not found"));
        }

        return new GetNamespaceResponse { Name = config.Name, Backend = config.Backend };
    }

    public override async Task<ListNamespacesResponse> ListNamespaces(
        ListNamespacesRequest request,
        ServerCallContext context)
    {
        var namespaces = await _registry.ListAsync(context.CancellationToken);
        var response = new ListNamespacesResponse();
        foreach (var ns in namespaces)
        {
            response.Namespaces.Add(new NamespaceEntry { Name = ns.Name, Backend = ns.Backend });
        }

        return response;
    }

    public override async Task<UpdateNamespaceResponse> UpdateNamespace(
        UpdateNamespaceRequest request,
        ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "name is required"));
        }

        ValidateNamespace(request.Name);
        var backendName = ResolveBackend(request.Backend);

        var existing = await _registry.GetAsync(request.Name, context.CancellationToken);
        if (existing == null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"namespace '{request.Name}' not found"));
        }

        var config = new NamespaceConfig { Name = request.Name, Backend = backendName };
        await _registry.UpdateAsync(config, context.CancellationToken);

        if (existing.Backend != backendName)
        {
            await _coordinator.CloseLocalNamespaceAsync(request.Name);
        }

        return new UpdateNamespaceResponse();
    }

    public override async Task<DeleteNamespaceResponse> DeleteNamespace(
        DeleteNamespaceRequest request,
        ServerCallContext context)
    {
        ValidateNamespace(request.Name);
        var ns = request.Name;
        NamespaceConfig? config = null;

        try
        {
            config = await _registry.GetAsync(ns, context.CancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read namespace '{Namespace}' before delete; continuing cleanup", ns);
        }

        try
        {
            await _registry.DeleteAsync(ns, context.CancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete namespace '{Namespace}' from registry; continuing cleanup", ns);
        }

        try
        {
            await _coordinator.ReleaseNamespaceAsync(ns);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to release namespace '{Namespace}' from coordinator; continuing cleanup", ns);
        }

        try
        {
            await _pool.DeleteDataAsync(ns, config?.Backend);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete physical data for namespace '{Namespace}'", ns);
        }

        return new DeleteNamespaceResponse();
    }

    public override async Task<HibernateNamespaceResponse> HibernateNamespace(
        HibernateNamespaceRequest request,
        ServerCallContext context)
    {
        ValidateNamespace(request.Name);
        await _coordinator.ReleaseNamespaceAsync(request.Name);

        return new HibernateNamespaceResponse();
    }

    private static void ValidateNamespace(string namespaceName)
    {
        if (!NamespaceNames.IsValid(namespaceName))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, NamespaceNames.ValidationMessage));
        }
    }

    private string ResolveBackend(string? requested)
    {
        var name = string.IsNullOrWhiteSpace(requested) || requested == "default"
            ? _clusterConfig.DefaultBackend ?? "local"
            : requested;

        if (!_backends.Backends.TryGetValue(name, out var config))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"unknown backend: {name}"));
        }

        if (_clusterConfig.ClusterMode && config.Type == BackendType.Local)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "local backend is not allowed in cluster mode"));
        }

        return name;
    }
}
