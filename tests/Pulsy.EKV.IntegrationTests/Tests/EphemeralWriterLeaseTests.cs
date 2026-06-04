using System.Text;
using System.Text.Json;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NATS.Client.KeyValueStore;
using Pulsy.EKV.Client.Models;
using Pulsy.EKV.IntegrationTests.Infrastructure;
using Pulsy.EKV.Node.Cluster;
using Pulsy.EKV.Node.Cluster.Leasing;
using Pulsy.EKV.Node.Cluster.Namespaces;
using Pulsy.EKV.Node.Cluster.Registry;
using Pulsy.EKV.Node.Configuration;
using Pulsy.EKV.Node.Models;
using Pulsy.EKV.Node.Storage.DatabasePool;
using Xunit;

namespace Pulsy.EKV.IntegrationTests.Tests;

[Collection(ClusterCollection.Name)]
public sealed class EphemeralWriterLeaseTests
{
    private readonly ClusterFixture _fixture;

    public EphemeralWriterLeaseTests(ClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task FirstRequestCanOpenNamespaceWithoutPreassignedOwner()
    {
        var ns = $"lazy-open-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceRegistryEntryAsync(ns, TestContext.Current.CancellationToken);

        Assert.False(_fixture.Node1.IsNamespaceOpen(ns));
        Assert.False(_fixture.Node2.IsNamespaceOpen(ns));

        var kv = _fixture.Client2.Namespace(ns);
        await kv.PutAsync("key", "value"u8.ToArray(), TestContext.Current.CancellationToken);

        var value = await kv.GetAsync("key", TestContext.Current.CancellationToken);

        Assert.NotNull(value);
        Assert.Equal("value", Encoding.UTF8.GetString(value));
        Assert.False(_fixture.Node1.IsNamespaceOpen(ns));
        Assert.True(_fixture.Node2.IsNamespaceOpen(ns));
    }

    [Fact]
    public async Task WriterLeasePayloadContainsCurrentEndpoint()
    {
        var ns = $"endpoint-lease-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceRegistryEntryAsync(ns, TestContext.Current.CancellationToken);

        await _fixture.Client2.Namespace(ns)
            .PutAsync("key", "value"u8.ToArray(), TestContext.Current.CancellationToken);

        var kv = _fixture.Node2.Services.GetRequiredService<NatsKVContext>();
        var store = await kv.GetStoreAsync(NatsBuckets.NamespaceLeases, TestContext.Current.CancellationToken);
        var entry = await store.TryGetEntryAsync<string>(
            "ns." + ns,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(entry.Success);

        using var doc = JsonDocument.Parse(entry.Value.Value!);
        Assert.Equal(_fixture.Node2.NodeId, doc.RootElement.GetProperty("nodeId").GetString());
        Assert.Equal(
            $"http://localhost:{_fixture.Node2.Port}",
            doc.RootElement.GetProperty("endpoint").GetString());
        Assert.True(doc.RootElement.GetProperty("expiresAtUtc").GetDateTimeOffset() > DateTimeOffset.UtcNow);
    }

    [Fact]
    public async Task StaleLocalReleaseDoesNotDeleteLeaseOwnedByAnotherNode()
    {
        var ns = $"cas-release-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceRegistryEntryAsync(ns, TestContext.Current.CancellationToken);

        await _fixture.Client1.Namespace(ns)
            .PutAsync("key", "value"u8.ToArray(), TestContext.Current.CancellationToken);

        var kv = _fixture.Node1.Services.GetRequiredService<NatsKVContext>();
        var store = await kv.GetStoreAsync(NatsBuckets.NamespaceLeases, TestContext.Current.CancellationToken);
        var current = await store.TryGetEntryAsync<string>(
            "ns." + ns,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(current.Success);

        var replacementLease = JsonSerializer.Serialize(new
        {
            nodeId = _fixture.Node2.NodeId,
            endpoint = $"http://localhost:{_fixture.Node2.Port}",
            acquiredAtUtc = DateTimeOffset.UtcNow,
            renewedAtUtc = DateTimeOffset.UtcNow,
        });

        await store.UpdateAsync(
            "ns." + ns,
            replacementLease,
            current.Value.Revision,
            cancellationToken: TestContext.Current.CancellationToken);

        var staleLeaseManager = _fixture.Node1.Services.GetRequiredService<ILeaseManager>();
        await staleLeaseManager.ReleaseAsync(ns, TestContext.Current.CancellationToken);

        var afterRelease = await store.TryGetEntryAsync<string>(
            "ns." + ns,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(afterRelease.Success);

        using var doc = JsonDocument.Parse(afterRelease.Value.Value!);
        Assert.Equal(_fixture.Node2.NodeId, doc.RootElement.GetProperty("nodeId").GetString());
    }

    [Fact]
    public async Task AdminUpdateOnNonOwnerDoesNotOpenSlateDbWithoutLease()
    {
        var ns = $"admin-update-non-owner-{Guid.NewGuid():N}";

        await _fixture.Node1.CreateNamespaceAsync(ns, TestContext.Current.CancellationToken);

        Assert.True(_fixture.Node1.IsNamespaceOpen(ns));
        Assert.False(_fixture.Node2.IsNamespaceOpen(ns));

        await _fixture.Client2.Admin().UpdateNamespaceAsync(
            new NamespaceInfo { Name = ns, Backend = "remote-test" },
            TestContext.Current.CancellationToken);

        Assert.True(_fixture.Node1.IsNamespaceOpen(ns));
        Assert.False(_fixture.Node2.IsNamespaceOpen(ns));
    }

    [Fact]
    public async Task AdminUpdateOnNonOwnerReleasesCurrentOwnerOnRenew()
    {
        var ns = $"admin-update-release-owner-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, TestContext.Current.CancellationToken);

        var leaseManager = _fixture.Node1.Services.GetRequiredService<ILeaseManager>();
        Assert.True(leaseManager.IsOwnedLocally(ns));

        await _fixture.Client2.Admin().UpdateNamespaceAsync(
            new NamespaceInfo { Name = ns, Backend = "remote-test" },
            TestContext.Current.CancellationToken);

        await WaitUntilAsync(
            () => !leaseManager.IsOwnedLocally(ns),
            "owner to release namespace after registry backend changed");
    }

    [Fact]
    public async Task AdminDeleteOnNonOwnerReleasesCurrentOwnerOnRenew()
    {
        var ns = $"admin-delete-release-owner-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, TestContext.Current.CancellationToken);

        var leaseManager = _fixture.Node1.Services.GetRequiredService<ILeaseManager>();
        Assert.True(leaseManager.IsOwnedLocally(ns));

        await _fixture.Client1.Namespace(ns)
            .PutAsync("key", "value"u8.ToArray(), TestContext.Current.CancellationToken);

        await _fixture.Client2.Admin()
            .DeleteNamespaceAsync(ns, TestContext.Current.CancellationToken);

        await WaitUntilAsync(
            () => !leaseManager.IsOwnedLocally(ns),
            "owner to release namespace after registry entry was deleted");

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => _fixture.Client1.Namespace(ns).GetAsync("key", TestContext.Current.CancellationToken));
        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
    }

    [Fact]
    public async Task AdminDeleteUsesRegistryBackendForPhysicalCleanupAfterRelease()
    {
        var ns = $"admin-delete-backend-{Guid.NewGuid():N}";
        var registry = _fixture.Node1.Services.GetRequiredService<INamespaceRegistry>();
        await registry.CreateAsync(
            new NamespaceConfig { Name = ns, Backend = "remote-test" },
            TestContext.Current.CancellationToken);

        var nodeConfig = _fixture.Node1.Services.GetRequiredService<IOptions<NodeConfig>>().Value;
        var localDir = Path.Combine(Path.GetFullPath(nodeConfig.DataPath), ns);
        Directory.CreateDirectory(localDir);

        await _fixture.Client1.Admin().DeleteNamespaceAsync(ns, TestContext.Current.CancellationToken);

        Assert.True(Directory.Exists(localDir));
    }

    [Fact]
    public async Task SameOwnerCanReacquireWhenLocalTtlExpiresBeforeKvCleanup()
    {
        var ns = $"same-owner-reacquire-{Guid.NewGuid():N}";
        var kv = _fixture.Node1.Services.GetRequiredService<NatsKVContext>();
        var leaseManager = new NatsLeaseManager(
            kv,
            Options.Create(new NodeConfig
            {
                Id = "reacquiring-owner",
                DataPath = Path.Combine(Path.GetTempPath(), "ekv-reacquiring-owner"),
                GrpcEndpoint = $"http://localhost:{_fixture.Node1.Port}",
            }),
            Options.Create(new ClusterConfig { LeaseTtlSeconds = 1 }),
            NullLogger<NatsLeaseManager>.Instance);

        Assert.True(await leaseManager.TryAcquireAsync(ns, TestContext.Current.CancellationToken));

        await kv.CreateOrUpdateStoreAsync(
            new NatsKVConfig(NatsBuckets.NamespaceLeases)
            {
                MaxAge = TimeSpan.FromSeconds(30),
            },
            TestContext.Current.CancellationToken);

        await Task.Delay(TimeSpan.FromMilliseconds(1_200), TestContext.Current.CancellationToken);

        Assert.False(leaseManager.IsOwnedLocally(ns));
        Assert.True(await leaseManager.TryAcquireAsync(ns, TestContext.Current.CancellationToken));
        Assert.True(leaseManager.IsOwnedLocally(ns));
    }

    [Fact]
    public async Task ConcurrentFirstRequestsOpenOnlyOneWriterAcrossNodes()
    {
        var ns = $"concurrent-open-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceRegistryEntryAsync(ns, TestContext.Current.CancellationToken);

        var node1Kv = _fixture.Client1.Namespace(ns);
        var node2Kv = _fixture.Client2.Namespace(ns);

        await Task.WhenAll(
            node1Kv.PutAsync("from-node-1", "value-1"u8.ToArray(), TestContext.Current.CancellationToken),
            node2Kv.PutAsync("from-node-2", "value-2"u8.ToArray(), TestContext.Current.CancellationToken));

        var value1 = await node1Kv.GetAsync("from-node-2", TestContext.Current.CancellationToken);
        var value2 = await node2Kv.GetAsync("from-node-1", TestContext.Current.CancellationToken);

        Assert.NotNull(value1);
        Assert.NotNull(value2);
        Assert.Equal("value-2", Encoding.UTF8.GetString(value1));
        Assert.Equal("value-1", Encoding.UTF8.GetString(value2));

        var node1Opened = _fixture.Node1.IsNamespaceOpen(ns);
        var node2Opened = _fixture.Node2.IsNamespaceOpen(ns);
        Assert.NotEqual(node1Opened, node2Opened);

        using var lease = await ReadLeaseDocumentAsync(ns);
        var owner = lease.RootElement.GetProperty("nodeId").GetString();
        Assert.True(owner == _fixture.Node1.NodeId || owner == _fixture.Node2.NodeId);
        Assert.Equal(owner == _fixture.Node1.NodeId, node1Opened);
        Assert.Equal(owner == _fixture.Node2.NodeId, node2Opened);
    }

    [Fact]
    public async Task ExpiredJsonLeaseFromDeadNodeCanBeClaimedBeforeKvCleanup()
    {
        var ns = $"expired-remote-lease-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceRegistryEntryAsync(ns, TestContext.Current.CancellationToken);

        var now = DateTimeOffset.UtcNow;
        var staleLease = JsonSerializer.Serialize(new
        {
            nodeId = "dead-node",
            endpoint = "http://localhost:1",
            acquiredAtUtc = now.AddMinutes(-10),
            renewedAtUtc = now.AddMinutes(-10),
            expiresAtUtc = now.AddSeconds(-1),
        });

        var store = await GetLeaseStoreAsync();
        await store.PutAsync("ns." + ns, staleLease, cancellationToken: TestContext.Current.CancellationToken);

        await _fixture.Client2.Namespace(ns)
            .PutAsync("key", "value"u8.ToArray(), TestContext.Current.CancellationToken);

        using var lease = await ReadLeaseDocumentAsync(ns);
        Assert.Equal(_fixture.Node2.NodeId, lease.RootElement.GetProperty("nodeId").GetString());
        Assert.True(_fixture.Node2.IsNamespaceOpen(ns));
    }

    [Fact]
    public async Task JsonLeaseWithoutEndpointFailsFastWithoutNodeStatusFallback()
    {
        var ns = $"missing-endpoint-lease-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, TestContext.Current.CancellationToken);
        await WaitForNodeStatusAsync(_fixture.Node1.NodeId);

        var now = DateTimeOffset.UtcNow;
        var store = await GetLeaseStoreAsync();
        var current = await store.TryGetEntryAsync<string>(
            "ns." + ns,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(current.Success);

        var leaseWithoutEndpoint = JsonSerializer.Serialize(new
        {
            nodeId = _fixture.Node1.NodeId,
            acquiredAtUtc = now.AddMinutes(-1),
            renewedAtUtc = now,
            expiresAtUtc = now.AddMinutes(5),
        });

        await store.UpdateAsync(
            "ns." + ns,
            leaseWithoutEndpoint,
            current.Value.Revision,
            cancellationToken: TestContext.Current.CancellationToken);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(2));

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => _fixture.Client2.Namespace(ns).PutAsync("key", "value"u8.ToArray(), cts.Token));

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.False(_fixture.Node2.IsNamespaceOpen(ns));
    }

    [Fact]
    public async Task BareStringLeasePayloadFailsFastWithoutNodeStatusFallback()
    {
        var ns = $"bare-string-lease-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, TestContext.Current.CancellationToken);

        var store = await GetLeaseStoreAsync();
        var current = await store.TryGetEntryAsync<string>(
            "ns." + ns,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(current.Success);

        await store.UpdateAsync(
            "ns." + ns,
            _fixture.Node1.NodeId,
            current.Value.Revision,
            cancellationToken: TestContext.Current.CancellationToken);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(2));

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => _fixture.Client2.Namespace(ns).PutAsync("key", "value"u8.ToArray(), cts.Token));

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.False(_fixture.Node2.IsNamespaceOpen(ns));
    }

    [Fact]
    public async Task MalformedLeasePayloadFailsFastWithoutOpeningLocalWriter()
    {
        var ns = $"malformed-lease-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceRegistryEntryAsync(ns, TestContext.Current.CancellationToken);

        var store = await GetLeaseStoreAsync();
        await store.PutAsync(
            "ns." + ns,
            "{not-json",
            cancellationToken: TestContext.Current.CancellationToken);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(2));

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => _fixture.Client2.Namespace(ns).PutAsync("key", "value"u8.ToArray(), cts.Token));

        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.False(_fixture.Node2.IsNamespaceOpen(ns));
    }

    [Fact]
    public async Task ClosingNamespaceBlocksReopenUntilActiveHandleIsReleased()
    {
        var ns = $"close-drain-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, TestContext.Current.CancellationToken);

        var pool = _fixture.Node1.Services.GetRequiredService<DatabasePool>();
        var coordinator = _fixture.Node1.Services.GetRequiredService<NamespaceCoordinator>();
        using var activeHandle = pool.Acquire(ns);
        Assert.NotNull(activeHandle);

        var closeTask = pool.CloseAsync(ns);
        await Task.Delay(TimeSpan.FromMilliseconds(5_500), TestContext.Current.CancellationToken);

        Assert.False(closeTask.IsCompleted);

        var reopenTask = coordinator.GetStoreAsync(ns, TestContext.Current.CancellationToken);
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.False(reopenTask.IsCompleted);

        activeHandle.Dispose();
        await closeTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        using var reopened = await reopenTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.NotNull(reopened);
    }

    [Fact]
    public async Task ReleaseNamespaceRejectsReopenUntilLeaseIsReleased()
    {
        var ns = $"release-drain-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, TestContext.Current.CancellationToken);

        var pool = _fixture.Node1.Services.GetRequiredService<DatabasePool>();
        var coordinator = _fixture.Node1.Services.GetRequiredService<NamespaceCoordinator>();
        var leaseManager = _fixture.Node1.Services.GetRequiredService<ILeaseManager>();
        using var activeHandle = pool.Acquire(ns);
        Assert.NotNull(activeHandle);

        var releaseTask = coordinator.ReleaseNamespaceAsync(ns);
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.False(releaseTask.IsCompleted);

        var duringRelease = await coordinator.GetStoreAsync(ns, TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        Assert.Null(duringRelease);

        activeHandle.Dispose();
        await releaseTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.False(leaseManager.IsOwnedLocally(ns));

        using var reopened = await coordinator.GetStoreAsync(ns, TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.NotNull(reopened);
        Assert.True(leaseManager.IsOwnedLocally(ns));
    }

    [Fact]
    public async Task ReleaseNamespaceKeepsLeaseUntilActiveHandleIsDrained()
    {
        var ns = $"release-renew-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, TestContext.Current.CancellationToken);

        DateTimeOffset renewedBeforeRelease;
        using (var lease = await ReadLeaseDocumentAsync(ns))
        {
            renewedBeforeRelease = lease.RootElement.GetProperty("renewedAtUtc").GetDateTimeOffset();
        }

        var pool = _fixture.Node1.Services.GetRequiredService<DatabasePool>();
        var coordinator = _fixture.Node1.Services.GetRequiredService<NamespaceCoordinator>();
        var leaseManager = _fixture.Node1.Services.GetRequiredService<ILeaseManager>();
        using var activeHandle = pool.Acquire(ns);
        Assert.NotNull(activeHandle);

        var releaseTask = coordinator.ReleaseNamespaceAsync(ns);
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.False(releaseTask.IsCompleted);

        Assert.True(await leaseManager.TryRenewAsync(ns, TestContext.Current.CancellationToken));

        using (var lease = await ReadLeaseDocumentAsync(ns))
        {
            var renewedAt = lease.RootElement.GetProperty("renewedAtUtc").GetDateTimeOffset();
            Assert.True(renewedAt > renewedBeforeRelease);
        }

        activeHandle.Dispose();
        await releaseTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        var store = await GetLeaseStoreAsync();
        var afterRelease = await store.TryGetEntryAsync<string>(
            "ns." + ns,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.False(afterRelease.Success);
    }

    [Fact]
    public async Task LocalOwnershipExpiresWhenLeaseTtlPassesWithoutRenewal()
    {
        var ns = $"local-expiry-{Guid.NewGuid():N}";
        var kv = _fixture.Node1.Services.GetRequiredService<NatsKVContext>();
        var leaseManager = new NatsLeaseManager(
            kv,
            Options.Create(new NodeConfig
            {
                Id = "short-lived-owner",
                DataPath = Path.Combine(Path.GetTempPath(), "ekv-short-lived-owner"),
                GrpcEndpoint = $"http://localhost:{_fixture.Node1.Port}",
            }),
            Options.Create(new ClusterConfig { LeaseTtlSeconds = 1 }),
            NullLogger<NatsLeaseManager>.Instance);

        Assert.True(await leaseManager.TryAcquireAsync(ns, TestContext.Current.CancellationToken));
        Assert.True(leaseManager.IsOwnedLocally(ns));

        await Task.Delay(TimeSpan.FromMilliseconds(1_200), TestContext.Current.CancellationToken);

        Assert.False(leaseManager.IsOwnedLocally(ns));
        Assert.DoesNotContain(ns, leaseManager.OwnedNamespaces);
    }

    private async Task<INatsKVStore> GetLeaseStoreAsync()
    {
        var kv = _fixture.Node1.Services.GetRequiredService<NatsKVContext>();
        return await kv.CreateOrUpdateStoreAsync(
            new NatsKVConfig(NatsBuckets.NamespaceLeases)
            {
                MaxAge = TimeSpan.FromSeconds(30),
            },
            TestContext.Current.CancellationToken);
    }

    private async Task<JsonDocument> ReadLeaseDocumentAsync(string namespaceName)
    {
        var store = await GetLeaseStoreAsync();
        var entry = await store.TryGetEntryAsync<string>(
            "ns." + namespaceName,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(entry.Success);
        return JsonDocument.Parse(entry.Value.Value!);
    }

    private async Task WaitForNodeStatusAsync(string nodeId)
    {
        var kv = _fixture.Node1.Services.GetRequiredService<NatsKVContext>();
        var statusStore = await kv.GetStoreAsync(NatsBuckets.NodeStatus, TestContext.Current.CancellationToken);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);

        while (DateTimeOffset.UtcNow < deadline)
        {
            var status = await statusStore.TryGetEntryAsync<string>(
                nodeId,
                cancellationToken: TestContext.Current.CancellationToken);
            if (status.Success)
            {
                return;
            }

            await Task.Delay(200, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException($"Timed out waiting for node status '{nodeId}'");
    }

    private async Task WaitUntilAsync(Func<bool> predicate, string reason)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (predicate())
            {
                return;
            }

            await Task.Delay(200, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException($"Timed out waiting for {reason}");
    }
}
