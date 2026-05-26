namespace Pulsy.EKV.Node.Storage.DatabasePool;

public readonly record struct OpenNamespaceInfo(string Name, string BackendName);

internal readonly record struct OpenStoreSnapshot(string Name, SlateDbStore Store);
