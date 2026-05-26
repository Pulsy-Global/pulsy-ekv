using System.Text;
using Pulsy.EKV.IntegrationTests.Infrastructure;
using Xunit;

namespace Pulsy.EKV.IntegrationTests.Tests;

[Collection(ClusterCollection.Name)]
public sealed class BasicOperationsTests
{
    private readonly ClusterFixture _fixture;

    public BasicOperationsTests(ClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task PutGet_ReturnsStoredValue()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"basic-putget-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var kv = _fixture.Client1.Namespace(ns);

        var value = "hello"u8.ToArray();
        await kv.PutAsync("key1", value, ct);

        var result = await kv.GetAsync("key1", ct);

        Assert.NotNull(result);
        Assert.Equal("hello", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Get_NonExistentKey_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"basic-notfound-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var kv = _fixture.Client1.Namespace(ns);

        var result = await kv.GetAsync("no-such-key", ct);

        Assert.Null(result);
    }

    [Fact]
    public async Task PutWithTtl_ExpiresAfterDelay()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"basic-ttl-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.PutAsync("ttl-key", Encoding.UTF8.GetBytes("temp"), TimeSpan.FromSeconds(2), ct);

        var before = await kv.GetAsync("ttl-key", ct);
        Assert.NotNull(before);

        await Task.Delay(TimeSpan.FromSeconds(3), ct);

        var after = await kv.GetAsync("ttl-key", ct);
        Assert.Null(after);
    }

    [Fact]
    public async Task Delete_RemovesKey()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"basic-delete-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.PutAsync("del-key", Encoding.UTF8.GetBytes("value"), ct);
        await kv.DeleteAsync("del-key", ct);

        var result = await kv.GetAsync("del-key", ct);
        Assert.Null(result);
    }

    [Fact]
    public async Task BatchWrite_StoresMultipleKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"basic-batch-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.BatchAsync(
            b =>
            {
                b.Put("b1", "v1"u8.ToArray());
                b.Put("b2", "v2"u8.ToArray());
                b.Put("b3", "v3"u8.ToArray());
            },
            ct);

        var r1 = await kv.GetAsync("b1", ct);
        var r2 = await kv.GetAsync("b2", ct);
        var r3 = await kv.GetAsync("b3", ct);

        Assert.Equal("v1", Encoding.UTF8.GetString(r1!));
        Assert.Equal("v2", Encoding.UTF8.GetString(r2!));
        Assert.Equal("v3", Encoding.UTF8.GetString(r3!));
    }

    [Fact]
    public async Task MultiGet_ReturnsExistingKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"basic-mget-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.PutAsync("mg1", "a"u8.ToArray(), ct);
        await kv.PutAsync("mg2", "b"u8.ToArray(), ct);

        var results = await kv.MultiGetAsync(["mg1", "mg2", "mg3"], ct);

        Assert.Equal(2, results.Count);
        Assert.Equal("a", Encoding.UTF8.GetString(results["mg1"]));
        Assert.Equal("b", Encoding.UTF8.GetString(results["mg2"]));
        Assert.False(results.ContainsKey("mg3"));
    }

    [Fact]
    public async Task ScanPrefix_ReturnsMatchingKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"basic-scan-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.BatchAsync(
            b =>
            {
                b.Put("prefix:a", "1"u8.ToArray());
                b.Put("prefix:b", "2"u8.ToArray());
                b.Put("prefix:c", "3"u8.ToArray());
                b.Put("other:x", "4"u8.ToArray());
            },
            ct);

        var result = await kv.ScanPrefixAsync("prefix:", ct: ct);

        Assert.Equal(3, result.Items.Count);
        Assert.All(result.Items, item => Assert.StartsWith("prefix:", item.Key));
    }

    [Fact]
    public async Task ScanPrefix_WithNonPositiveLimit_ThrowsArgumentOutOfRange()
    {
        var ct = TestContext.Current.CancellationToken;
        var ns = $"basic-scan-limit-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns, ct);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.PutAsync("prefix:a", "1"u8.ToArray(), ct);

        var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => kv.ScanPrefixAsync("prefix:", limit: 0, ct: ct));
        Assert.Equal("limit", ex.ParamName);
    }
}
