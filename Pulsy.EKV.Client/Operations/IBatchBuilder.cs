namespace Pulsy.EKV.Client.Operations;

public interface IBatchBuilder
{
    void Put(string key, byte[] value);

    void Put(string key, byte[] value, TimeSpan ttl);

    void Delete(string key);
}
