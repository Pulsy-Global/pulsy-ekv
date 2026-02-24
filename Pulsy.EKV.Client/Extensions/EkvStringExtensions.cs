using System.Text;
using Pulsy.EKV.Client.Namespaces;
using Pulsy.EKV.Client.Operations;

namespace Pulsy.EKV.Client;

public static class EkvStringExtensions
{
    public static async Task<string?> GetStringAsync(
        this IEkvNamespace ns,
        string key,
        CancellationToken ct = default)
    {
        var bytes = await ns.GetAsync(key, ct);
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    public static Task PutAsync(
        this IEkvNamespace ns,
        string key,
        string value,
        CancellationToken ct = default) =>
        ns.PutAsync(key, Encoding.UTF8.GetBytes(value), ct);

    public static Task PutAsync(
        this IEkvNamespace ns,
        string key,
        string value,
        TimeSpan ttl,
        CancellationToken ct = default) =>
        ns.PutAsync(key, Encoding.UTF8.GetBytes(value), ttl, ct);

    public static async Task<IReadOnlyDictionary<string, string>> MultiGetStringsAsync(
        this IEkvNamespace ns,
        IReadOnlyList<string> keys,
        CancellationToken ct = default)
    {
        var raw = await ns.MultiGetAsync(keys, ct);
        var dict = new Dictionary<string, string>(raw.Count);
        foreach (var (k, v) in raw)
        {
            dict[k] = Encoding.UTF8.GetString(v);
        }

        return dict;
    }

    public static void Put(this IBatchBuilder builder, string key, string value) =>
        builder.Put(key, Encoding.UTF8.GetBytes(value));

    public static void Put(this IBatchBuilder builder, string key, string value, TimeSpan ttl) =>
        builder.Put(key, Encoding.UTF8.GetBytes(value), ttl);
}
