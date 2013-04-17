using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;

using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.UTest.Mocks
{
    class NodeQueueMock : IMemcacheRequestsQueue, IMemcacheNode
    {
        BlockingCollection<IMemcacheRequest> _queue = new BlockingCollection<IMemcacheRequest>();

        public IMemcacheRequest Take()
        {
            return _queue.Take();
        }

        public bool TryTake(out IMemcacheRequest request, int timeout)
        {
            return _queue.TryTake(out request, timeout);
        }

        public void Add(IMemcacheRequest request)
        {
            _queue.Add(request);
        }

#pragma warning disable 67
        public event Action<Exception> TransportError;
        public event Action<Headers.MemcacheResponseHeader, IMemcacheRequest> MemcacheError;
        public event Action<Headers.MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;
        public event Action<IMemcacheNode> NodeDead;
#pragma warning restore 67

        public System.Net.EndPoint EndPoint
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsDead
        {
            get { return false; }
        }

        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            return _queue.TryAdd(request, timeout);
        }

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}
