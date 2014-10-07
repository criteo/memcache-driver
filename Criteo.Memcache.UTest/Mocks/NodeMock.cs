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

using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.UTest.Mocks
{
    internal class NodeMock : IMemcacheNode
    {
#pragma warning disable 67
        public event Action<Exception> TransportError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;
        public event Action<IMemcacheNode> NodeAlive;
#pragma warning restore 67
        public event Action<IMemcacheNode> NodeDead;

        public System.Net.EndPoint EndPoint
        {
            get;
            set;
        }

        private bool _isDead;
        public bool IsDead
        {
            get { return _isDead; }
            set
            {
                _isDead = value;
                if (_isDead && NodeDead != null)
                    NodeDead(this);
                if (!_isDead && NodeDead != null)
                    NodeAlive(this);
            }
        }

        public Status DefaultResponse { get; set; }

        public IMemcacheRequest LastRequest { get; private set; }

        public static int trySendCounter;

        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            LastRequest = request;
            trySendCounter++;

            if (timeout == 0)
            {
                var response = new MemcacheResponseHeader
                {
                    Status = DefaultResponse,
                    ExtraLength = 4,
                };

                LastRequest.HandleResponse(response, request.Key, new byte[4], new byte[0]);
            }

            return true;
        }

        public bool Shutdown(bool force)
        {
            return true;
        }

        public void Dispose()
        {
        }
    }
}
