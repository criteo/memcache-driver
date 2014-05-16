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
using System.Net;
using System.Threading;

using Criteo.Memcache;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Transport;

namespace Criteo.Memcache.Node
{
    internal class MemcacheNode : IMemcacheNode
    {
        private readonly BlockingCollection<IMemcacheTransport> _transportPool;
        private volatile bool _isAlive = false;
        private volatile CancellationTokenSource _tokenSource;
        private MemcacheClientConfiguration _configuration;
        private volatile bool _ongoingDispose = false;
        private volatile bool _forceShutDown = false;
        private volatile bool _ongoingShutdown = false;

        private int _workingTransports;

        public int WorkingTransports { get { return _workingTransports; } }

        #region Events

        public event Action<Exception> TransportError;

        private void OnTransportError(Exception e)
        {
            if (TransportError != null)
                TransportError(e);
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;

        private void OnMemcacheError(MemcacheResponseHeader h, IMemcacheRequest e)
        {
            if (MemcacheError != null)
                MemcacheError(h, e);
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;

        private void OnMemcacheResponse(MemcacheResponseHeader h, IMemcacheRequest e)
        {
            if (MemcacheResponse != null)
                MemcacheResponse(h, e);
        }

        public event Action<IMemcacheNode> NodeDead;

        public event Action<IMemcacheNode> NodeAlive;

        // To be called in the transport constructor
        private void RegisterEvents(IMemcacheTransport transport)
        {
            transport.MemcacheError += OnMemcacheError;
            transport.MemcacheResponse += OnMemcacheResponse;
            transport.TransportError += OnTransportError;
            transport.TransportDead += OnTransportDead;
        }

        private void OnTransportDead(IMemcacheTransport transport)
        {
            lock (this)
            {
                transport.Registered = false;
                if (0 == Interlocked.Decrement(ref _workingTransports))
                {
                    _isAlive = false;
                    if (NodeDead != null)
                        NodeDead(this);

                    if(!_tokenSource.IsCancellationRequested)
                        _tokenSource.Cancel();
                }
            }
        }

        #endregion Events

        private readonly EndPoint _endPoint;

        public EndPoint EndPoint
        {
            get { return _endPoint; }
        }

        public MemcacheNode(EndPoint endPoint, MemcacheClientConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentException("Client config should not be null");

            _configuration = configuration;
            _endPoint = endPoint;
            _tokenSource = new CancellationTokenSource();
            _transportPool = new BlockingCollection<IMemcacheTransport>(new ConcurrentStack<IMemcacheTransport>());

            for (int i = 0; i < configuration.PoolSize; ++i)
            {
                var transport = (configuration.TransportFactory ?? MemcacheTransport.DefaultAllocator)
                                    (endPoint,
                                    configuration,
                                    RegisterEvents,
                                    TransportAvailable,
                                    false,
                                    IsClosing);
                TransportAvailable(transport);
            }
        }

        private void TransportAvailable(IMemcacheTransport transport)
        {
            try
            {
                if (!transport.Registered)
                {
                    transport.Registered = true;
                    Interlocked.Increment(ref _workingTransports);
                    if (!_isAlive)
                        lock (this)
                            if (!_isAlive)
                            {
                                _isAlive = true;
                                if (NodeAlive != null)
                                    NodeAlive(this);
                                _tokenSource = new CancellationTokenSource();
                            }
                }

                // Add the transport to the pool, unless the node is disposing of the pool
                if (!_ongoingDispose)
                {
                    _transportPool.Add(transport);
                    if (_ongoingShutdown)
                        transport.Shutdown(_forceShutDown);
                }
                else
                    transport.Dispose();
            }
            catch (Exception e)
            {
                if (TransportError != null)
                    TransportError(e);
            }
        }

        public bool IsDead
        {
            get { return !_isAlive; }
        }

        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            IMemcacheTransport transport;
            try
            {
                int tries = 0;
                while (!IsClosing()
                    && !_tokenSource.IsCancellationRequested
                    && ++tries <= _configuration.PoolSize
                    && _transportPool.TryTake(out transport, timeout, _tokenSource.Token))
                {
                    bool sent = transport.TrySend(request);

                    if (sent)
                        return true;
                }
            }
            catch (OperationCanceledException)
            {
                // someone called for a cancel on the pool.TryTake and already raised the problem
                return false;
            }

            return false;
        }

        public bool Shutdown(bool force)
        {
            _forceShutDown = force;
            _ongoingShutdown = true;

            foreach(var transport in _transportPool)
            {
                if (!transport.Shutdown(force))
                    return false;
            }

            return true;
        }

        internal bool IsClosing()
        {
            return _ongoingDispose || _ongoingShutdown;
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_ongoingDispose)
            {
                _ongoingDispose = true;

                if (disposing)
                {
                    // Release all threads blocking on the transport pool
                    try
                    {
                        if (_tokenSource != null)
                            _tokenSource.Cancel();
                    }
                    catch (Exception e)
                    {
                        OnTransportError(e);
                    }

                    // Dispose of the transports currently in the pool
                    IMemcacheTransport transport;
                    while (_transportPool.TryTake(out transport))
                        transport.Dispose();

                    // Let the GC finalize _tokenSource and _transportPool;
                }
            }
        }

        #endregion IDisposable

        // for testing purpose only !!!
        internal int PoolSize { get { return _transportPool.Count; } }

        public override string ToString()
        {
            return "MemcacheNode " + EndPoint;
        }
    }
}
