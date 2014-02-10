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
