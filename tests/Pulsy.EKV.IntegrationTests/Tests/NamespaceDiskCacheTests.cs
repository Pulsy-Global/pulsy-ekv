using Pulsy.EKV.Node.Configuration.Pool;
using Pulsy.EKV.Node.Storage.DatabasePool;
using Xunit;

namespace Pulsy.EKV.IntegrationTests.Tests;

public sealed class NamespaceDiskCacheTests
{
    [Fact]
    public void GetNamespaceRoot_UsesNamespaceAsDirectoryName()
    {
        var root = Path.Combine(Path.GetTempPath(), $"ekv-cache-{Guid.NewGuid():N}");
        var config = new DiskCacheConfig { RootFolder = root };

        var namespaceRoot = NamespaceDiskCache.GetNamespaceRoot(config, "tenant-1_users", Path.GetTempPath());

        Assert.Equal(Path.Combine(Path.GetFullPath(root), "tenant-1_users"), namespaceRoot);
    }

    [Fact]
    public void GetNamespaceRoot_RejectsPathSeparators()
    {
        var root = Path.Combine(Path.GetTempPath(), $"ekv-cache-{Guid.NewGuid():N}");
        var config = new DiskCacheConfig { RootFolder = root };

        var ex = Assert.Throws<ArgumentException>(
            () => NamespaceDiskCache.GetNamespaceRoot(config, "tenant-1/users", Path.GetTempPath()));

        Assert.Equal("namespaceName", ex.ParamName);
    }

    [Fact]
    public void GetNamespaceRoot_ResolvesRelativeRootUnderDataPath()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"ekv-data-{Guid.NewGuid():N}");
        var config = new DiskCacheConfig { RootFolder = "cache" };

        var namespaceRoot = NamespaceDiskCache.GetNamespaceRoot(config, "tenant-a", dataPath);

        Assert.StartsWith(Path.Combine(Path.GetFullPath(dataPath), "cache"), namespaceRoot);
    }

    [Fact]
    public void DeleteNamespaceCache_RemovesOnlyRequestedNamespaceCache()
    {
        var root = Path.Combine(Path.GetTempPath(), $"ekv-cache-{Guid.NewGuid():N}");
        var config = new DiskCacheConfig { RootFolder = root };
        var namespaceRoot = NamespaceDiskCache.GetNamespaceRoot(config, "tenant-a", Path.GetTempPath());
        var neighborRoot = NamespaceDiskCache.GetNamespaceRoot(config, "tenant-b", Path.GetTempPath());

        Directory.CreateDirectory(namespaceRoot);
        Directory.CreateDirectory(neighborRoot);
        File.WriteAllText(Path.Combine(namespaceRoot, "cached-object"), "cache");
        File.WriteAllText(Path.Combine(neighborRoot, "cached-object"), "cache");

        try
        {
            var deleted = NamespaceDiskCache.DeleteNamespaceCache(namespaceRoot);

            Assert.True(deleted);
            Assert.False(Directory.Exists(namespaceRoot));
            Assert.True(Directory.Exists(neighborRoot));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }
}
