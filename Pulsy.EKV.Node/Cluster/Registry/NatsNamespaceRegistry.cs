using System.Text.Json;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Node.Models;

namespace Pulsy.EKV.Node.Cluster.Registry;

public sealed class NatsNamespaceRegistry : INamespaceRegistry
{
    private readonly NatsKVContext _kv;
    private readonly ILogger<NatsNamespaceRegistry> _logger;
    private INatsKVStore? _store;

    public NatsNamespaceRegistry(NatsKVContext kv, ILogger<NatsNamespaceRegistry> logger)
    {
        _kv = kv;
        _logger = logger;
    }

    public async Task InitAsync(CancellationToken ct = default)
    {
        _store = await _kv.CreateOrUpdateStoreAsync(new NatsKVConfig(NatsBuckets.Namespaces), ct);
        _logger.LogInformation("Namespace registry initialized (bucket: {Bucket})", NatsBuckets.Namespaces);
    }

    public async Task<NamespaceConfig?> GetAsync(string name, CancellationToken ct = default)
    {
        var result = await _store!.TryGetEntryAsync<string>(name, cancellationToken: ct);
        if (!result.Success)
        {
            return null;
        }

        return JsonSerializer.Deserialize<NamespaceConfig>(result.Value.Value!);
    }

    public async Task<IReadOnlyList<NamespaceConfig>> ListAsync(CancellationToken ct = default)
    {
        var configs = new List<NamespaceConfig>();
        await foreach (var key in _store!.GetKeysAsync(cancellationToken: ct))
        {
            var entry = await _store.TryGetEntryAsync<string>(key, cancellationToken: ct);
            if (!entry.Success)
            {
                continue;
            }

            var config = JsonSerializer.Deserialize<NamespaceConfig>(entry.Value.Value!);
            if (config != null)
            {
                configs.Add(config);
            }
        }

        return configs;
    }

    public async Task CreateAsync(NamespaceConfig config, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(config);
        try
        {
            await _store!.CreateAsync(config.Name, json, cancellationToken: ct);
            _logger.LogInformation("Namespace created: {Name}", config.Name);
        }
        catch (NatsKVCreateException)
        {
            throw new InvalidOperationException($"Namespace already exists: {config.Name}");
        }
    }

    public async Task UpdateAsync(NamespaceConfig config, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(config);
        await _store!.PutAsync(config.Name, json, cancellationToken: ct);
        _logger.LogInformation("Namespace updated: {Name}", config.Name);
    }

    public async Task DeleteAsync(string name, CancellationToken ct = default)
    {
        await _store!.PurgeAsync(name, cancellationToken: ct);
        _logger.LogInformation("Namespace deleted: {Name}", name);
    }
}
