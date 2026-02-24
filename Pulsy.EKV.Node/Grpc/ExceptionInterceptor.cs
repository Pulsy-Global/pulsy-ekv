using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Pulsy.EKV.Node.Grpc;

public sealed class ExceptionInterceptor : Interceptor
{
    private readonly ILogger<ExceptionInterceptor> _logger;

    public ExceptionInterceptor(ILogger<ExceptionInterceptor> logger)
    {
        _logger = logger;
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (ArgumentException ex)
        {
            _logger.LogDebug(ex, "Invalid argument in {Method}", context.Method);
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            await continuation(request, responseStream, context);
        }
        catch (ArgumentException ex)
        {
            _logger.LogDebug(ex, "Invalid argument in {Method}", context.Method);
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }
}
