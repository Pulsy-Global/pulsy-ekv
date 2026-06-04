using System.Text;
using Pulsy.EKV.IntegrationTests.Infrastructure;
using Xunit;

namespace Pulsy.EKV.IntegrationTests.Tests;

[Collection(ClusterCollection.Name)]
public sealed class ForwardingTests
{
    private readonly ClusterFixture _fixture;

    public ForwardingTests(ClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task CrossNodeGetAfterPut()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"fwd-get-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var ns1 = _fixture.Client1.Namespace(ns);
        var ns2 = _fixture.Client2.Namespace(ns);

        await ns1.PutAsync("cross-key", "cross-value"u8.ToArray(), ct);

        // Read from node-2
        var result = await ns2.GetAsync("cross-key", ct);

        Assert.NotNull(result);
        Assert.Equal("cross-value", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task CrossNodeScan()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"fwd-scan-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var ns1 = _fixture.Client1.Namespace(ns);
        var ns2 = _fixture.Client2.Namespace(ns);

        await ns1.BatchAsync(
            b =>
            {
                b.Put("scan:1", "a"u8.ToArray());
                b.Put("scan:2", "b"u8.ToArray());
                b.Put("scan:3", "c"u8.ToArray());
            },
            ct);

        // Scan from node-2
        var result = await ns2.ScanPrefixAsync("scan:", ct: ct);

        Assert.Equal(3, result.Items.Count);
    }

    [Fact]
    public async Task CrossNodeDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"fwd-del-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var ns1 = _fixture.Client1.Namespace(ns);
        var ns2 = _fixture.Client2.Namespace(ns);

        await ns1.PutAsync("del-key", "value"u8.ToArray(), ct);

        // Delete from node-2
        await ns2.DeleteAsync("del-key", ct);

        // Verify from node-1
        var result = await ns1.GetAsync("del-key", ct);
        Assert.Null(result);
    }
}
