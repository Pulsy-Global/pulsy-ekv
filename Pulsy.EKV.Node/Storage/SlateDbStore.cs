using System.Text;
using Pulsy.EKV.Grpc;
using Pulsy.EKV.Node.Models;
using Pulsy.SlateDB;
using Pulsy.SlateDB.Options;

namespace Pulsy.EKV.Node.Storage;

public sealed class SlateDbStore : IDisposable
{
    private readonly SlateDb _db;

    public SlateDbStore(SlateDb db)
    {
        _db = db;
    }

    public byte[]? Get(string key) => _db.Get(key);

    public void Put(string key, byte[] value, TimeSpan? ttl = null, bool awaitDurable = true)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var writeOpts = new WriteOptions { AwaitDurable = awaitDurable };
        var putOpts = ttl.HasValue ? PutOptions.ExpireAfter(ttl.Value) : PutOptions.NoExpiry;
        _db.Put(keyBytes, value, putOpts, writeOpts);
    }

    public void Delete(string key, bool awaitDurable = true)
        => _db.Delete(key, new WriteOptions { AwaitDurable = awaitDurable });

    public void WriteBatch(IReadOnlyList<BatchEntry> entries, bool awaitDurable = true)
    {
        var batch = SlateDb.NewWriteBatch();
        foreach (var entry in entries)
        {
            if (entry.Type == BatchOpType.Delete)
            {
                batch.Delete(entry.Key);
            }
            else
            {
                if (entry.Ttl.HasValue)
                {
                    batch.Put(entry.Key, entry.Value!, PutOptions.ExpireAfter(entry.Ttl.Value));
                }
                else
                {
                    batch.Put(entry.Key, entry.Value!);
                }
            }
        }

        _db.Write(batch, new WriteOptions { AwaitDurable = awaitDurable });
    }

    public List<KeyValuePair<string, byte[]>> MultiGet(IReadOnlyList<string> keys)
    {
        var results = new List<KeyValuePair<string, byte[]>>(keys.Count);
        foreach (var key in keys)
        {
            var value = _db.Get(key);
            if (value != null)
            {
                results.Add(new KeyValuePair<string, byte[]>(key, value));
            }
        }

        return results;
    }

    public StringKeyIterator CreatePrefixIterator(string prefix) => new(_db.ScanPrefix(prefix));

    public StringKeyIterator CreateScanIterator(string? startKey, string? endKey) => new(_db.Scan(startKey, endKey));

    public string Metrics() => _db.Metrics();

    public void Flush() => _db.Flush();

    public void Dispose() => _db.Dispose();

    public sealed class StringKeyIterator : IDisposable
    {
        private readonly SlateDbScanIterator _inner;

        public StringKeyIterator(SlateDbScanIterator inner) => _inner = inner;

        public void Seek(string key) => _inner.Seek(Encoding.UTF8.GetBytes(key));

        public KeyValuePair<string, byte[]>? Next()
        {
            var kv = _inner.Next();
            if (kv == null)
            {
                return null;
            }

            return new KeyValuePair<string, byte[]>(Encoding.UTF8.GetString(kv.Key), kv.Value);
        }

        public void Dispose() => _inner.Dispose();
    }
}
