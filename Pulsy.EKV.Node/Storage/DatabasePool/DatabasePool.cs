using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Configuration.Backends;
using Pulsy.EKV.Node.Configuration.Pool;
using Pulsy.EKV.Node.Diagnostics;
using Pulsy.SlateDB;
using Pulsy.SlateDB.Options;

namespace Pulsy.EKV.Node.Storage.DatabasePool;

public sealed class DatabasePool : IHostedService
{
    private const string LocalStorageUrlScheme = "file:///";

    private readonly ConcurrentDictionary<string, PoolEntry> _entries = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _namespaceLocks = new();
    private readonly SemaphoreSlim _openLock = new(1, 1);
    private readonly NodeConfig _nodeConfig;
    private readonly PoolConfig _poolConfig;
    private readonly BackendsConfig _backends;
    private readonly ILogger<DatabasePool> _logger;
    private readonly TimeSpan _evictionInterval;
    private Timer? _evictionTimer;
    private EkvMetrics? _metrics;

    public DatabasePool(
        IOptions<NodeConfig> nodeConfig,
        IOptions<PoolConfig> poolConfig,
        IOptions<BackendsConfig> backends,
        ILogger<DatabasePool> logger)
    {
        _nodeConfig = nodeConfig.Value;
        _poolConfig = poolConfig.Value;
        _backends = backends.Value;
        _logger = logger;
        _evictionInterval = TimeSpan.FromSeconds(_poolConfig.EvictionIntervalSeconds);
    }

    public bool WalEnabled => _poolConfig.WalEnabled;

    public int OpenCount => _entries.Count;

    public Task StartAsync(CancellationToken ct)
    {
        _evictionTimer = new Timer(
            EvictIdle,
            null,
            _evictionInterval,
            _evictionInterval);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken ct)
    {
        if (_evictionTimer != null)
        {
            await _evictionTimer.DisposeAsync();
        }

        _evictionTimer = null;
        await FlushAndCloseAllAsync();
        _openLock.Dispose();
    }

    public IReadOnlyList<OpenNamespaceInfo> ListOpenNamespaces()
        => _entries.Select(e => new OpenNamespaceInfo(e.Key, e.Value.BackendName)).ToList();

    public StoreHandle? Acquire(string namespaceName)
    {
        if (_entries.TryGetValue(namespaceName, out var entry))
        {
            entry.IncrementOps();

            if (!_entries.TryGetValue(namespaceName, out var current) || !ReferenceEquals(current, entry))
            {
                entry.DecrementOps();
                return null;
            }

            entry.LastAccess = DateTime.UtcNow;
            return new StoreHandle(entry.Store, () =>
            {
                entry.LastAccess = DateTime.UtcNow;
                entry.DecrementOps();
            });
        }

        return null;
    }

    public async Task<SlateDbStore> GetOrOpenAsync(string namespaceName, string backendName)
    {
        if (_entries.TryGetValue(namespaceName, out var existing))
        {
            existing.LastAccess = DateTime.UtcNow;
            return existing.Store;
        }

        var namespaceLock = GetNamespaceLock(namespaceName);
        await namespaceLock.WaitAsync();
        try
        {
            return await GetOrOpenUnderNamespaceLockAsync(namespaceName, backendName);
        }
        finally
        {
            namespaceLock.Release();
        }
    }

    public async Task CloseAsync(string namespaceName)
    {
        var namespaceLock = GetNamespaceLock(namespaceName);
        await namespaceLock.WaitAsync();
        try
        {
            PoolEntry entry;
            await _openLock.WaitAsync();
            try
            {
                if (!_entries.TryRemove(namespaceName, out entry!))
                {
                    return;
                }
            }
            finally
            {
                _openLock.Release();
            }

            _logger.LogInformation("Closing SlateDB for namespace {Namespace}", namespaceName);
            await WaitForActiveOperationsAsync(namespaceName, entry);
            await Task.Run(() => CloseEntry(namespaceName, entry, flush: true));
        }
        finally
        {
            namespaceLock.Release();
        }
    }

    public async Task DeleteDataAsync(string namespaceName, string? backendName = null)
    {
        if (backendName == null && _entries.TryGetValue(namespaceName, out var entry))
        {
            backendName = entry.BackendName;
        }

        await CloseAsync(namespaceName);

        var backend = backendName != null && _backends.Backends.TryGetValue(backendName, out var cfg)
            ? cfg
            : null;

        if (backend?.Type == BackendType.S3)
        {
            _logger.LogWarning(
                "Namespace {Namespace} uses S3 backend {Backend} - data in bucket {Bucket} must be cleaned via lifecycle rules or manual deletion",
                namespaceName,
                backendName,
                backend.Bucket);
            return;
        }

        var nsDir = Path.Combine(Path.GetFullPath(_nodeConfig.DataPath), namespaceName);
        if (Directory.Exists(nsDir))
        {
            await Task.Run(() => Directory.Delete(nsDir, recursive: true));
            _logger.LogInformation("Deleted data for namespace {Namespace} at {Path}", namespaceName, nsDir);
        }
    }

    internal void SetMetrics(EkvMetrics metrics) => _metrics = metrics;

    internal IReadOnlyList<OpenStoreSnapshot> ListOpenStores()
        => _entries.Select(e => new OpenStoreSnapshot(e.Key, e.Value.Store)).ToList();

    private async Task<SlateDbStore> GetOrOpenUnderNamespaceLockAsync(string namespaceName, string backendName)
    {
        if (_entries.TryGetValue(namespaceName, out var existing))
        {
            existing.LastAccess = DateTime.UtcNow;
            return existing.Store;
        }

        await _openLock.WaitAsync();
        try
        {
            if (_entries.TryGetValue(namespaceName, out existing))
            {
                existing.LastAccess = DateTime.UtcNow;
                return existing.Store;
            }

            if (_entries.Count >= _poolConfig.MaxOpen)
            {
                EvictLru();
            }

            var backend = ResolveBackend(backendName);

            var diskCacheRoot = backend.Type == BackendType.S3 && _poolConfig.DiskCache.Enabled
                ? NamespaceDiskCache.GetNamespaceRoot(_poolConfig.DiskCache, namespaceName, _nodeConfig.DataPath)
                : null;

            SlateDb db;
            var maxRetries = _poolConfig.OpenRetryMaxAttempts;
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    db = backend.Type switch
                    {
                        BackendType.S3 => await Task.Run(() => OpenS3(namespaceName, backend, diskCacheRoot)),
                        _ => await Task.Run(() => OpenLocal(namespaceName)),
                    };
                    break;
                }
                catch (SlateDbException ex) when (attempt < maxRetries && ex.Message.Contains("newer DB client"))
                {
                    _logger.LogWarning(
                        ex,
                        "Fencing error opening namespace {Namespace}, retrying ({Attempt}/{Max})",
                        namespaceName,
                        attempt,
                        maxRetries);
                    await Task.Delay(_poolConfig.OpenRetryBaseDelayMs * attempt);
                }
                catch (SlateDbException ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to open database for namespace {Namespace} (backend: {Backend})",
                        namespaceName,
                        backendName);
                    throw;
                }
            }

            var store = new SlateDbStore(db);
            var entry = new PoolEntry(store, backendName, backend.Type, diskCacheRoot);

            _entries[namespaceName] = entry;
            return store;
        }
        finally
        {
            _openLock.Release();
        }
    }

    private SemaphoreSlim GetNamespaceLock(string namespaceName)
        => _namespaceLocks.GetOrAdd(namespaceName, _ => new SemaphoreSlim(1, 1));

    private async Task FlushAndCloseAllAsync()
    {
        _logger.LogInformation("Flushing and closing {Count} open databases", _entries.Count);
        var tasks = _entries.Select(kvp => Task.Run(() => CloseEntry(kvp.Key, kvp.Value, flush: true)));
        await Task.WhenAll(tasks);
        _entries.Clear();
    }

    private async Task WaitForActiveOperationsAsync(string namespaceName, PoolEntry entry)
    {
        var activeOps = entry.ActiveOps;
        if (activeOps == 0)
        {
            return;
        }

        _logger.LogInformation(
            "Waiting for {ActiveOps} active operation(s) before closing namespace {Namespace}",
            activeOps,
            namespaceName);

        await entry.WaitForIdleAsync();
    }

    private void CloseEntry(string namespaceName, PoolEntry entry, bool flush)
    {
        if (flush)
        {
            try
            {
                entry.Store.Flush();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error flushing namespace {Namespace}, proceeding to close", namespaceName);
            }
        }

        try
        {
            entry.Store.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error closing namespace {Namespace}", namespaceName);
        }

        DropDiskCacheAfterClose(namespaceName, entry);
    }

    private void DropDiskCacheAfterClose(string namespaceName, PoolEntry entry)
    {
        if (entry.BackendType != BackendType.S3 || entry.DiskCacheRoot == null)
        {
            return;
        }

        try
        {
            var deleted = NamespaceDiskCache.DeleteNamespaceCache(entry.DiskCacheRoot);
            if (!deleted)
            {
                return;
            }

            _logger.LogInformation(
                "Dropped disk cache for namespace {Namespace} at {Path}",
                namespaceName,
                entry.DiskCacheRoot);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to drop disk cache for namespace {Namespace} at {Path}",
                namespaceName,
                entry.DiskCacheRoot);
        }
    }

    private BackendConfig ResolveBackend(string name)
    {
        if (!_backends.Backends.TryGetValue(name, out var cfg))
        {
            throw new InvalidOperationException($"Unknown backend: {name}");
        }

        return cfg;
    }

    private SlateDb OpenLocal(string namespaceName)
    {
        var dataDir = Path.GetFullPath(_nodeConfig.DataPath);
        Directory.CreateDirectory(dataDir);
        var url = LocalStorageUrlScheme + dataDir;
        _logger.LogInformation("Opening SlateDB for namespace {Namespace} at {Url}", namespaceName, url);
        return SlateDb.Builder(namespaceName, url).WithSettings(BuildSettings()).Build();
    }

    private SlateDb OpenS3(string namespaceName, BackendConfig cfg, string? diskCacheRoot)
    {
        _logger.LogInformation(
            "Opening SlateDB for namespace {Namespace} on S3 bucket {Bucket}",
            namespaceName,
            cfg.Bucket);

        var builder = SlateDb.Builder(namespaceName, new ObjectStoreConfig
        {
            Bucket = cfg.Bucket!,
            Region = cfg.Region,
            Endpoint = cfg.Endpoint,
            AccessKeyId = cfg.AccessKeyId,
            SecretAccessKey = cfg.SecretAccessKey,
            AllowHttp = cfg.AllowHttp,
        });

        return builder.WithSettings(BuildSettings(diskCacheRoot)).Build();
    }

    private SlateDbSettings BuildSettings(string? diskCacheRoot = null)
    {
        var bf = _poolConfig.BloomFilter;
        var compactor = _poolConfig.Compactor;
        var settings = new SlateDbSettings
        {
            MinFilterKeys = (uint)bf.MinFilterKeys,
            FilterBitsPerKey = (uint)bf.BitsPerKey,
            CompressionCodec = _poolConfig.Compression,
            L0MaxSsts = (ulong)_poolConfig.L0MaxSsts,
            L0SstSizeBytes = (ulong)_poolConfig.L0SstSizeBytes,
            MaxUnflushedBytes = (ulong)_poolConfig.MaxUnflushedBytes,
            WalEnabled = _poolConfig.WalEnabled,
            ManifestPollInterval = TimeSpan.FromSeconds(_poolConfig.ManifestPollIntervalSeconds),
            CompactorOptions = new CompactorOptions
            {
                PollInterval = TimeSpan.FromSeconds(_poolConfig.CompactorPollIntervalSeconds),
                MaxConcurrentCompactions = (ulong)compactor.MaxConcurrentCompactions,
                MaxSstSize = (ulong)compactor.MaxSstSizeBytes,
            },
            GarbageCollectorOptions = new GarbageCollectorOptions(),
        };

        var dc = _poolConfig.DiskCache;
        if (diskCacheRoot != null && dc.Enabled)
        {
            var budgetPerInstance = dc.TotalDiskBudgetMb > 0 && _poolConfig.MaxOpen > 0
                ? dc.TotalDiskBudgetMb / _poolConfig.MaxOpen
                : (int?)null;

            var perInstanceMb = (dc.MaxSizeMb, budgetPerInstance) switch
            {
                (int max, int budget) => Math.Min(max, budget),
                (int max, null) => max,
                (null, int budget) => budget,
                (null, null) => dc.FallbackSizeMb,
            };

            settings = settings with
            {
                CacheOptions = new CacheOptions
                {
                    RootFolder = diskCacheRoot,
                    MaxCacheSizeBytes = (ulong)Math.Max(1, perInstanceMb) * 1024 * 1024,
                    CachePuts = dc.CachePuts,
                    PreloadDiskCacheOnStartup = dc.PreloadL0 ? PreloadLevel.L0Sst : null,
                },
            };
        }

        return settings;
    }

    private void EvictIdle(object? state)
    {
        _ = EvictIdleAsync().ContinueWith(
            t => _logger.LogError(t.Exception, "Unhandled error in idle eviction"),
            TaskContinuationOptions.OnlyOnFaulted);
    }

    private async Task EvictIdleAsync()
    {
        var cutoff = DateTime.UtcNow.AddSeconds(-_poolConfig.IdleTimeoutSeconds);
        foreach (var (name, entry) in _entries)
        {
            if (entry.LastAccess >= cutoff)
            {
                continue;
            }

            await _openLock.WaitAsync();
            try
            {
                if (!_entries.TryGetValue(name, out var current))
                {
                    continue;
                }

                if (current.LastAccess >= cutoff || current.ActiveOps > 0)
                {
                    continue;
                }

                if (_entries.TryRemove(name, out var removed))
                {
                    if (removed.ActiveOps > 0)
                    {
                        _entries[name] = removed;
                        continue;
                    }

                    _metrics?.RecordEviction();
                    _logger.LogInformation("Evicting idle namespace {Namespace}", name);
                    CloseEntry(name, removed, flush: false);
                }
            }
            finally
            {
                _openLock.Release();
            }
        }
    }

    private void EvictLru()
    {
        var oldest = _entries
            .Where(e => e.Value.ActiveOps == 0)
            .OrderBy(e => e.Value.LastAccess)
            .FirstOrDefault();

        if (oldest.Key != null && _entries.TryRemove(oldest.Key, out var entry))
        {
            if (entry.ActiveOps > 0)
            {
                _entries[oldest.Key] = entry;
                return;
            }

            _metrics?.RecordEviction();
            _logger.LogInformation("Evicting LRU namespace {Namespace}", oldest.Key);

            CloseEntry(oldest.Key, entry, flush: false);
        }
    }
}
