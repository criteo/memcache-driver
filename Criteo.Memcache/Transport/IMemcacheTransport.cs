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

        IMemcacheRequestsQueue RequestsQueue { get; }
    }
}
