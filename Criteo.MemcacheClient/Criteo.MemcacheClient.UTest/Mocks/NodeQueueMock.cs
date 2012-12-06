using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Node;
using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.UTest.Mocks
{
    class NodeQueueMock : IMemcacheNodeQueue
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
    }
}
