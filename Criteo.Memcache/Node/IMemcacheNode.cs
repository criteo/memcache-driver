/* Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/
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

        /// <summary>
        /// Attempt to shutdown the node and the transports associated with it.
        /// May return false ifsome requests are still pending, allowing for a graceful shutdown.
        /// </summary>
        /// <param name="force">Force an immediate shutdown and fail all pending requests.</param>
        /// <returns>True if the node was successfully shut down.</returns>
        bool Shutdown(bool force);
    }
}
