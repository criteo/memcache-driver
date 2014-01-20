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
﻿using System;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Transport;

namespace Criteo.Memcache.UTest.Mocks
{
    internal class TransportMock : IMemcacheTransport
    {
        private bool _isAlive;
        public bool IsAlive
        {
            get { return _isAlive; }
            set
            {
                if (!_isAlive && value && Setup != null)
                    Setup(this);

                if (_isAlive && !value && TransportDead != null)
                {
                    TransportDead(this);

                    TransportError = null;
                    MemcacheError = null;
                    MemcacheResponse = null;
                    TransportDead = null;
                }

                _isAlive = value;
            }
        }

        public bool Registered { get; set; }

        public Action<IMemcacheTransport> Setup;
        public MemcacheResponseHeader Response { private get; set; }
        public byte[] Extra { private get; set; }
        public byte[] Message { private get; set; }

        public TransportMock(Action<IMemcacheTransport> registerEvents)
        {
            registerEvents(this);
        }


        public bool TrySend(IMemcacheRequest req)
        {
            if (!IsAlive)
            {
                return false;
            }
            else
            {
                req.HandleResponse(Response, null, Extra, Message);
                return true;
            }
        }

        public void Kill()
        {
        }

#pragma warning disable 67

        public event Action<Exception> TransportError;

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;

        public event Action<IMemcacheTransport> TransportDead;

#pragma warning restore 67

        public void Dispose()
        {
        }
    }
}
