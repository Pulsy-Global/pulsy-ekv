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

        if (_clusterConfig.ClusterMode && string.IsNullOrWhiteSpace(request.Backend))
        {
            throw new RpcException(
                new Status(
                    StatusCode.InvalidArgument,
                    "backend is required in cluster mode"));
        }

        var backend = string.IsNullOrWhiteSpace(request.Backend) ? "local" : request.Backend;
        if (!_backends.Backends.TryGetValue(backend, out var backendsBackend))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"unknown backend: {backend}"));
        }

        if (_clusterConfig.ClusterMode && backendsBackend.Type == BackendType.Local)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "local backend is not allowed in cluster mode"));
        }

        var ns = request.Name;
        var ct = context.CancellationToken;

        var existing = await _registry.GetAsync(ns, ct);
        if (existing != null)
        {
            throw new RpcException(
                new Status(
                    StatusCode.AlreadyExists,
                    $"Namespace '{ns}' already exists"));
        }

        var config = new NamespaceConfig { Name = ns, Backend = backend };
        await _registry.CreateAsync(config, ct);

        var store = await _coordinator.TryClaimAsync(ns, ct);
        if (store == null)
        {
            throw new RpcException(
                new Status(
                    StatusCode.Internal,
                    $"namespace '{ns}' created but could not be claimed by this node"));
        }

        return new CreateNamespaceResponse();
    }

    public override async Task<GetNamespaceResponse> GetNamespace(
        GetNamespaceRequest request,
        ServerCallContext context)
    {
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

        if (!_backends.Backends.TryGetValue(request.Backend, out var backend))
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"backend '{request.Backend}' not found"));
        }

        if (_clusterConfig.ClusterMode && backend.Type == BackendType.Local)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "local backend is not allowed in cluster mode"));
        }

        var existing = await _registry.GetAsync(request.Name, context.CancellationToken);
        if (existing == null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"namespace '{request.Name}' not found"));
        }

        // Close the existing DB handle before updating the registry so that the
        // next request opens a store against the new backend config.
        await _pool.CloseAsync(request.Name);

        // NOTE: There is a brief window between CloseAsync and the next GetOrOpenAsync
        // where a concurrent request may fail to acquire a handle. This is acceptable
        // as the namespace will be re-opened on the next request.

        var config = new NamespaceConfig { Name = request.Name, Backend = request.Backend };
        await _registry.UpdateAsync(config, context.CancellationToken);

        if (existing.Backend != request.Backend)
        {
            await _pool.GetOrOpenAsync(request.Name, request.Backend);
        }

        return new UpdateNamespaceResponse();
    }

    public override async Task<DeleteNamespaceResponse> DeleteNamespace(
        DeleteNamespaceRequest request,
        ServerCallContext context)
    {
        var ns = request.Name;

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
            await _pool.DeleteDataAsync(ns);
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
        await _coordinator.ReleaseNamespaceAsync(request.Name);
        await _pool.CloseAsync(request.Name);

        return new HibernateNamespaceResponse();
    }
}
