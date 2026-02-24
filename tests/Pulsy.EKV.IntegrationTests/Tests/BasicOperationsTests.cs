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
        var ns = $"basic-putget-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns);
        var kv = _fixture.Client1.Namespace(ns);

        var value = "hello"u8.ToArray();
        await kv.PutAsync("key1", value);

        var result = await kv.GetAsync("key1");

        Assert.NotNull(result);
        Assert.Equal("hello", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Get_NonExistentKey_ReturnsNull()
    {
        var ns = $"basic-notfound-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns);
        var kv = _fixture.Client1.Namespace(ns);

        var result = await kv.GetAsync("no-such-key");

        Assert.Null(result);
    }

    [Fact]
    public async Task PutWithTtl_ExpiresAfterDelay()
    {
        var ns = $"basic-ttl-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.PutAsync("ttl-key", Encoding.UTF8.GetBytes("temp"), TimeSpan.FromSeconds(2));

        var before = await kv.GetAsync("ttl-key");
        Assert.NotNull(before);

        await Task.Delay(TimeSpan.FromSeconds(3));

        var after = await kv.GetAsync("ttl-key");
        Assert.Null(after);
    }

    [Fact]
    public async Task Delete_RemovesKey()
    {
        var ns = $"basic-delete-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.PutAsync("del-key", Encoding.UTF8.GetBytes("value"));
        await kv.DeleteAsync("del-key");

        var result = await kv.GetAsync("del-key");
        Assert.Null(result);
    }

    [Fact]
    public async Task BatchWrite_StoresMultipleKeys()
    {
        var ns = $"basic-batch-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.BatchAsync(b =>
        {
            b.Put("b1", "v1"u8.ToArray());
            b.Put("b2", "v2"u8.ToArray());
            b.Put("b3", "v3"u8.ToArray());
        });

        var r1 = await kv.GetAsync("b1");
        var r2 = await kv.GetAsync("b2");
        var r3 = await kv.GetAsync("b3");

        Assert.Equal("v1", Encoding.UTF8.GetString(r1!));
        Assert.Equal("v2", Encoding.UTF8.GetString(r2!));
        Assert.Equal("v3", Encoding.UTF8.GetString(r3!));
    }

    [Fact]
    public async Task MultiGet_ReturnsExistingKeys()
    {
        var ns = $"basic-mget-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.PutAsync("mg1", "a"u8.ToArray());
        await kv.PutAsync("mg2", "b"u8.ToArray());

        var results = await kv.MultiGetAsync(["mg1", "mg2", "mg3"]);

        Assert.Equal(2, results.Count);
        Assert.Equal("a", Encoding.UTF8.GetString(results["mg1"]));
        Assert.Equal("b", Encoding.UTF8.GetString(results["mg2"]));
        Assert.False(results.ContainsKey("mg3"));
    }

    [Fact]
    public async Task ScanPrefix_ReturnsMatchingKeys()
    {
        var ns = $"basic-scan-{Guid.NewGuid():N}";
        await _fixture.Node1.CreateNamespaceAsync(ns);
        var kv = _fixture.Client1.Namespace(ns);

        await kv.BatchAsync(b =>
        {
            b.Put("prefix:a", "1"u8.ToArray());
            b.Put("prefix:b", "2"u8.ToArray());
            b.Put("prefix:c", "3"u8.ToArray());
            b.Put("other:x", "4"u8.ToArray());
        });

        var result = await kv.ScanPrefixAsync("prefix:");

        Assert.Equal(3, result.Items.Count);
        Assert.All(result.Items, item => Assert.StartsWith("prefix:", item.Key));
    }
}
