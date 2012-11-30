using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Requests;
using Criteo.MemcacheClient.Sockets;

namespace Criteo.MemcacheClient.UTest.Mocks
{
    /// <summary>
    /// Doesn't do anything, should simulate a dead socket
    /// </summary>
    class DeadSocketMock : IMemcacheSocket
    {
        public event Action<Exception> TransportError;

        public event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheError;

        public event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheResponse;


        public BlockingCollection<IMemcacheRequest> WaitingRequests { get; set; }
        public void RespondToRequest()
        {
            IMemcacheRequest request;
            while(WaitingRequests.TryTake(out request, 0))
                request.HandleResponse(new MemacheResponseHeader { }, null);
        }
    }
}
