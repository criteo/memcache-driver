using System;
using System.Net;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.Node
{
    public interface IMemcacheNode : IDisposable
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
        /// This event is triggered whenever the node goes dead
        /// </summary>
        event Action<IMemcacheNode> NodeDead;

        /// <summary>
        /// This event is triggered whenever the node goes alive
        /// </summary>
        event Action<IMemcacheNode> NodeAlive;

        /// <summary>
        /// The node endpoint
        /// </summary>
        EndPoint EndPoint { get; }

        /// <summary>
        /// Must returns true when the node is unreachable for a durable time
        /// </summary>
        bool IsDead { get; }

        /// <summary>
        /// Try to send a request through the node
        /// </summary>
        /// <param name="request">The request to send</param>
        /// <param name="timeout">Timeout.Infinite means no timeout</param>
        /// <returns>false if it failed</returns>
        bool TrySend(IMemcacheRequest request, int timeout);
    }
}
