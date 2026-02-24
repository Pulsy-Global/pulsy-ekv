using System.Net;
using System.Net.Sockets;

namespace Pulsy.EKV.IntegrationTests.Infrastructure;

internal static class PortAllocator
{
    public static int GetAvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();

        return port;
    }
}
