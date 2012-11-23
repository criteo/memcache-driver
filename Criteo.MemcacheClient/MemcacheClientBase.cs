using System;
using System.Collections.Concurrent;

using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient
{
    public abstract class MemcacheClientBase
    {
        protected abstract BlockingCollection<IMemcacheRequest> WaitingRequests { get; }

        public abstract event Action<Status, byte[]> OnResponse;

        public int QueueLength { get { return WaitingRequests == null ? 0 : WaitingRequests.Count; } }

        public void Set(string key, byte[] message, TimeSpan expiration)
        {
            WaitingRequests.Add(new SetRequest { Key = key, Message = message, Expire = expiration });
        }

        public void Get(string key, Action<byte[]> callback)
        {
            WaitingRequests.Add(new GetRequest { Key = key, Callback = callback });
        }
    }
}
