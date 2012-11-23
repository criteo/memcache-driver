using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.Node
{
    public interface IMemcacheNode
    {
        /// <summary>
        /// This event is triggered when an exception occures
        /// </summary>
        event Action<Exception> TransportError;

        /// <summary>
        /// This event is triggered every time a respond from the server is not ok
        /// </summary>
        event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheError;

        /// <summary>
        /// This event is triggered at every incoming response from the server
        /// </summary>
        event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheResponse;

        /// <summary>
        /// This event is triggered whenever the node goes dead
        /// </summary>
        event Action<MemcacheNode> NodeDead;

        /// <summary>
        /// This event is raised when the node asks for a manual flush of it's own queue
        /// </summary>
        event Action<MemcacheNode> NodeFlush;

        /// <summary>
        /// Must returns true when the node is unreachable for a durable time
        /// </summary>
        bool IsDead { get; }

        /// <summary>
        /// The node queue for pushing requests
        /// </summary>
        BlockingCollection<IMemcacheRequest> WaitingRequests { get; }
    }
}
