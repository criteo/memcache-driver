using System;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;

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

        event Action<IMemcacheTransport> TransportDead;

        // The Registered property is set to true by the memcache node
        // when it acknowledges the transport is a working transport.
        bool Registered { get; set; }

        /// <summary>
        /// Sends the request
        /// </summary>
        /// <param name="req">The request to send</param>
        /// <returns>false if the request has synchronously failed (then it won't call any callback)</returns>
        bool TrySend(IMemcacheRequest req);

        bool IsAlive { get; }
    }
}
