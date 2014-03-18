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
using System.Collections.Concurrent;

using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.UTest.Mocks
{
    class NodeQueueMock : IMemcacheNode
    {
        BlockingCollection<IMemcacheRequest> _queue = new BlockingCollection<IMemcacheRequest>();

        public IMemcacheRequest Take()
        {
            return _queue.Take();
        }

        public bool TryTake(out IMemcacheRequest request, int timeout)
        {
            return _queue.TryTake(out request, timeout);
        }

        public void Add(IMemcacheRequest request)
        {
            _queue.Add(request);
        }

#pragma warning disable 67
        public event Action<Exception> TransportError;
        public event Action<Headers.MemcacheResponseHeader, IMemcacheRequest> MemcacheError;
        public event Action<Headers.MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;
        public event Action<IMemcacheNode> NodeDead;
        public event Action<IMemcacheNode> NodeAlive;
#pragma warning restore 67

        public System.Net.EndPoint EndPoint
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsDead
        {
            get { return false; }
        }

        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            return _queue.TryAdd(request, timeout);
        }

        public bool Shutdown(bool force)
        {
            return true;
        }

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}
