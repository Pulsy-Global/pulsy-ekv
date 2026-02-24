using Grpc.Core;
using Grpc.Net.Client;
using Pulsy.EKV.Client.Models;
using Pulsy.EKV.Grpc;
using GrpcEkvAdmin = Pulsy.EKV.Grpc.EkvAdmin;

namespace Pulsy.EKV.Client.Admin;

public sealed class EkvAdmin : IEkvAdmin
{
    private readonly GrpcEkvAdmin.EkvAdminClient _client;

    public EkvAdmin(GrpcChannel channel)
    {
        _client = new GrpcEkvAdmin.EkvAdminClient(channel);
    }

    public async Task CreateNamespaceAsync(NamespaceInfo config, CancellationToken ct = default)
    {
        await _client.CreateNamespaceAsync(
            new CreateNamespaceRequest
            {
                Name = config.Name,
                Backend = config.Backend,
            },
            cancellationToken: ct);
    }

    public async Task EnsureNamespaceAsync(NamespaceInfo config, CancellationToken ct = default)
    {
        try
        {
            await CreateNamespaceAsync(config, ct);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
        {
        }
    }

    public async Task<NamespaceInfo?> GetNamespaceAsync(string name, CancellationToken ct = default)
    {
        try
        {
            var resp = await _client.GetNamespaceAsync(
                new GetNamespaceRequest { Name = name },
                cancellationToken: ct);
            return new NamespaceInfo { Name = resp.Name, Backend = resp.Backend };
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<NamespaceInfo>> ListNamespacesAsync(CancellationToken ct = default)
    {
        var resp = await _client.ListNamespacesAsync(
            new ListNamespacesRequest(),
            cancellationToken: ct);

        return resp.Namespaces
            .Select(n => new NamespaceInfo { Name = n.Name, Backend = n.Backend })
            .ToList();
    }

    public async Task UpdateNamespaceAsync(NamespaceInfo config, CancellationToken ct = default)
    {
        await _client.UpdateNamespaceAsync(
            new UpdateNamespaceRequest
            {
                Name = config.Name,
                Backend = config.Backend,
            },
            cancellationToken: ct);
    }

    public async Task HibernateNamespaceAsync(string name, CancellationToken ct = default)
    {
        await _client.HibernateNamespaceAsync(
            new HibernateNamespaceRequest { Name = name },
            cancellationToken: ct);
    }

    public async Task DeleteNamespaceAsync(string name, CancellationToken ct = default)
    {
        await _client.DeleteNamespaceAsync(
            new DeleteNamespaceRequest { Name = name },
            cancellationToken: ct);
    }
}
