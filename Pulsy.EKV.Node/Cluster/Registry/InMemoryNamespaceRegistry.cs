using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Models;

namespace Pulsy.EKV.Node.Cluster.Registry;

public sealed class InMemoryNamespaceRegistry : INamespaceRegistry
{
    private const string NamespacesPersistenceFileName = "namespaces.json";

    private readonly ConcurrentDictionary<string, NamespaceConfig> _namespaces = new();
    private readonly string _filePath;
    private readonly ILogger<InMemoryNamespaceRegistry> _logger;
    private readonly SemaphoreSlim _persistLock = new(1, 1);

    public InMemoryNamespaceRegistry(IOptions<NodeConfig> nodeConfig, ILogger<InMemoryNamespaceRegistry> logger)
    {
        _filePath = Path.Combine(nodeConfig.Value.DataPath, NamespacesPersistenceFileName);
        _logger = logger;
    }

    public async Task InitAsync(CancellationToken ct = default)
    {
        if (File.Exists(_filePath))
        {
            var json = await File.ReadAllTextAsync(_filePath, ct);
            var configs = JsonSerializer.Deserialize<List<NamespaceConfig>>(json);
            if (configs != null)
            {
                foreach (var config in configs)
                {
                    _namespaces[config.Name] = config;
                }
            }

            _logger.LogInformation("Loaded {Count} namespaces from {Path}", _namespaces.Count, _filePath);
        }
        else
        {
            _logger.LogInformation("No persisted namespaces found, starting empty");
        }
    }

    public Task<NamespaceConfig?> GetAsync(string name, CancellationToken ct = default)
    {
        _namespaces.TryGetValue(name, out var config);
        return Task.FromResult(config);
    }

    public Task<IReadOnlyList<NamespaceConfig>> ListAsync(CancellationToken ct = default)
    {
        IReadOnlyList<NamespaceConfig> list = _namespaces.Values.ToList();
        return Task.FromResult(list);
    }

    public async Task CreateAsync(NamespaceConfig config, CancellationToken ct = default)
    {
        if (!_namespaces.TryAdd(config.Name, config))
        {
            throw new InvalidOperationException($"Namespace already exists: {config.Name}");
        }

        _logger.LogInformation("Namespace created: {Name}", config.Name);
        await PersistAsync(ct);
    }

    public async Task UpdateAsync(NamespaceConfig config, CancellationToken ct = default)
    {
        _namespaces[config.Name] = config;
        _logger.LogInformation("Namespace updated: {Name}", config.Name);
        await PersistAsync(ct);
    }

    public async Task DeleteAsync(string name, CancellationToken ct = default)
    {
        _namespaces.TryRemove(name, out _);
        _logger.LogInformation("Namespace deleted: {Name}", name);
        await PersistAsync(ct);
    }

    private async Task PersistAsync(CancellationToken ct)
    {
        await _persistLock.WaitAsync(ct);
        try
        {
            var json = JsonSerializer.Serialize(_namespaces.Values.ToList());
            var dir = Path.GetDirectoryName(_filePath)!;
            Directory.CreateDirectory(dir);
            await File.WriteAllTextAsync(_filePath, json, ct);
        }
        finally
        {
            _persistLock.Release();
        }
    }
}
