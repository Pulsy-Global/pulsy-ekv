using Pulsy.EKV.Node.Configuration.Pool;
using Pulsy.EKV.Node.Models;

namespace Pulsy.EKV.Node.Storage.DatabasePool;

internal static class NamespaceDiskCache
{
    public static string GetNamespaceRoot(DiskCacheConfig config, string namespaceName, string dataPath)
    {
        ArgumentNullException.ThrowIfNull(config);
        NamespaceNames.ThrowIfInvalid(namespaceName, nameof(namespaceName));
        ArgumentException.ThrowIfNullOrWhiteSpace(dataPath);

        var root = ResolveRootFolder(config.RootFolder, dataPath);

        return Path.Combine(root, namespaceName);
    }

    public static bool DeleteNamespaceCache(string namespaceRoot)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(namespaceRoot);

        var fullPath = Path.GetFullPath(namespaceRoot);
        if (!Directory.Exists(fullPath))
        {
            return false;
        }

        Directory.Delete(fullPath, recursive: true);
        return true;
    }

    private static string ResolveRootFolder(string rootFolder, string dataPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootFolder);

        if (Path.IsPathFullyQualified(rootFolder))
        {
            return Path.GetFullPath(rootFolder);
        }

        return Path.GetFullPath(Path.Combine(dataPath, rootFolder));
    }
}
