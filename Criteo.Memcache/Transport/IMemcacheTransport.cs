using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;

namespace Criteo.Memcache.Transport
{
    public interface IMemcacheTransport : IDisposable
    {
        /// <summary>
        /// This event is triggered when an exception occures
        /// </summary>
        event Action<Exception> TransportError;

        /// <summary>
        /// This event is triggered every time a respond from the server is not ok
        /// </summary>
        event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;

        /// <summary>
        /// This event is triggered at every incoming response from the server
        /// </summary>
        event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;

        /// <summary>
        /// Sends the request
        /// </summary>
        /// <param name="req">The request to send</param>
        /// <returns>false if the request has synchronously failed (then it won't call any callback)</returns>
        bool TrySend(IMemcacheRequest req);

        /// <summary>
        /// Asks the transport to call for the setup action when ready to work
        /// (used to put it back in a pool after a failure)
        /// </summary>
        void PlanSetup();
    }
}
