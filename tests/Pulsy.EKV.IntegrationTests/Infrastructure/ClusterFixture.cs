using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Pulsy.EKV.Client;
using Pulsy.EKV.Client.Configuration;
using Xunit;

namespace Pulsy.EKV.IntegrationTests.Infrastructure;

public sealed class ClusterFixture : IAsyncLifetime
{
    private IContainer? _natsContainer;
    private string? _tempDir;

    public EkvNodeInstance Node1 { get; private set; } = null!;

    public EkvNodeInstance Node2 { get; private set; } = null!;

    public EkvClient Client1 { get; private set; } = null!;

    public EkvClient Client2 { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        SlateDB.SlateDb.InitLogging(SlateDB.Options.LogLevel.Warn);

        _natsContainer = new ContainerBuilder("nats:latest")
            .WithPortBinding(4222, true)
            .WithCommand("-js")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server is ready"))
            .Build();

        await _natsContainer.StartAsync();

        var natsPort = _natsContainer.GetMappedPublicPort(4222);
        var natsUrl = $"nats://localhost:{natsPort}";

        _tempDir = Path.Combine(Path.GetTempPath(), $"ekv-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        var port1 = PortAllocator.GetAvailablePort();
        var port2 = PortAllocator.GetAvailablePort();

        Node1 = EkvNodeInstance.Create("node-1", port1, _tempDir, natsUrl);
        Node2 = EkvNodeInstance.Create("node-2", port2, _tempDir, natsUrl);

        using var startupCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await Node1.StartAsync(startupCts.Token);
        await Node2.StartAsync(startupCts.Token);

        await WaitForHealthAsync(port1);
        await WaitForHealthAsync(port2);

        Client1 = new EkvClient(new EkvClientOptions { Endpoint = $"http://localhost:{port1}" });
        Client2 = new EkvClient(new EkvClientOptions { Endpoint = $"http://localhost:{port2}" });
    }

    public async ValueTask DisposeAsync()
    {
        Client1.Dispose();

        Client2.Dispose();

        try
        {
            await Node1.StopAsync();
        }
        catch
        {
            // ignored
        }

        await Node1.DisposeAsync();

        try
        {
            await Node2.StopAsync();
        }
        catch
        {
            // ignored
        }

        await Node2.DisposeAsync();

        if (_natsContainer != null)
        {
            await _natsContainer.StopAsync();
            await _natsContainer.DisposeAsync();
        }

        if (_tempDir != null && Directory.Exists(_tempDir))
        {
            try
            {
                Directory.Delete(_tempDir, recursive: true);
            }
            catch
            {
                // ignored
            }
        }
    }

    private static async Task WaitForHealthAsync(int port)
    {
        using var handler = new SocketsHttpHandler();
        using var client = new HttpClient(handler);
        var deadline = DateTime.UtcNow.AddSeconds(30);

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost:{port}/health")
                {
                    Version = new Version(2, 0),
                    VersionPolicy = HttpVersionPolicy.RequestVersionExact,
                };

                var response = await client.SendAsync(request);
                if (response.IsSuccessStatusCode)
                {
                    return;
                }
            }
            catch
            {
            }

            await Task.Delay(500);
        }

        throw new TimeoutException($"Node on port {port} did not become healthy within 30s");
    }
}
