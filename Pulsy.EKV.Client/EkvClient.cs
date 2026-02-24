using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Pulsy.EKV.Client.Admin;
using Pulsy.EKV.Client.Configuration;
using Pulsy.EKV.Client.Namespaces;

namespace Pulsy.EKV.Client;

public sealed class EkvClient : IEkvClient
{
    private readonly GrpcChannel _channel;

    public EkvClient(EkvClientOptions? options = null)
    {
        var opts = options ?? new EkvClientOptions();
        var serviceConfig = BuildServiceConfig(opts);

        _channel = GrpcChannel.ForAddress(
            opts.Endpoint,
            new GrpcChannelOptions
            {
                MaxReceiveMessageSize = opts.MaxMessageBytes,
                MaxSendMessageSize = opts.MaxMessageBytes,
                ServiceConfig = serviceConfig,
            });
    }

    public IEkvNamespace Namespace(string name) => new EkvNamespace(_channel, name);

    public IEkvAdmin Admin() => new EkvAdmin(_channel);

    public void Dispose() => _channel.Dispose();

    private static ServiceConfig BuildServiceConfig(EkvClientOptions opts) => new()
    {
        MethodConfigs =
        {
            new MethodConfig
            {
                Names = { MethodName.Default },
                RetryPolicy = new RetryPolicy
                {
                    MaxAttempts = opts.RetryMaxAttempts,
                    InitialBackoff = TimeSpan.FromMilliseconds(opts.RetryInitialBackoffMs),
                    MaxBackoff = TimeSpan.FromMilliseconds(opts.RetryMaxBackoffMs),
                    BackoffMultiplier = opts.RetryBackoffMultiplier,
                    RetryableStatusCodes = { StatusCode.Unavailable },
                },
            },
        },
    };
}
