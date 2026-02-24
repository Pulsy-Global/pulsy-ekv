using Pulsy.EKV.Client.Admin;
using Pulsy.EKV.Client.Namespaces;

namespace Pulsy.EKV.Client;

public interface IEkvClient : IDisposable
{
    IEkvNamespace Namespace(string name);

    IEkvAdmin Admin();
}
