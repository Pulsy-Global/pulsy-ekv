using System.Text;

namespace Pulsy.EKV.Client.Models;

public sealed record KvEntry
{
    public required string Key { get; init; }

    public required byte[] Value { get; init; }

    public string StringValue => Encoding.UTF8.GetString(Value);
}
