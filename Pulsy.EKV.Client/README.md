# Pulsy.EKV.Client

gRPC client for [Pulsy EKV](https://github.com/Pulsy-Global/pulsy-ekv) - a clustered key-value store backed by SlateDB.

## Usage

```csharp
using var client = new EkvClient(new EkvClientOptions
{
    Endpoint = "http://localhost:8080"
});

var ns = client.Namespace("my-namespace");

await ns.PutAsync("key", "value"u8.ToArray());
var value = await ns.GetAsync("key");
await ns.DeleteAsync("key");
```
