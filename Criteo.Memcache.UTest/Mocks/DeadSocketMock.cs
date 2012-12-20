using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Sockets;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;

namespace Criteo.Memcache.UTest.Mocks
{
    /// <summary>
    /// Doesn't do anything, should simulate a dead socket
    /// </summary>
    class DeadSocketMock : IMemcacheSocket
    {
        // I don't care of unsed event in my mocks ...
        public event Action<Exception> TransportError
        {
            add { }
            remove { }
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError
        {
            add { }
            remove { }
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse
        {
            add { }
            remove { }
        }

        public IMemcacheNodeQueue WaitingRequests { get; set; }
        public void RespondToRequest()
        {
            IMemcacheRequest request;
            while(WaitingRequests.TryTake(out request, 0))
                request.HandleResponse(new MemcacheResponseHeader {}, null, null);
        }

        public void Dispose()
        {
        }
    }
}
