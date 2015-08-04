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
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Exceptions;
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
        private readonly MemcacheClientConfiguration _configuration;
        private volatile bool _ongoingDispose = false;
        private volatile bool _forceShutDown = false;
        private Action _shutDownCallback = null;

        private int _workingTransports;

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

                    if (!_tokenSource.IsCancellationRequested)
                        _tokenSource.Cancel();

                    if (null != _shutDownCallback)
                        _shutDownCallback();
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
                (configuration.TransportFactory ?? MemcacheTransport.DefaultAllocator)
                    (endPoint, configuration, RegisterEvents, TransportAvailable, false, IsClosing);
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
                    if (_forceShutDown)
                        transport.Shutdown(null);
                    else if (null != _shutDownCallback)
                        transport.Shutdown(InternalShutdownCallback);
                    else
                        _transportPool.Add(transport);
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
            try
            {
                var tries = 0;
                IMemcacheTransport transport;
                while (!IsClosing()
                    && !_tokenSource.IsCancellationRequested
                    && ++tries <= _configuration.PoolSize
                    && _transportPool.TryTake(out transport, timeout, _tokenSource.Token))
                {
                    if (transport.TrySend(request))
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

        public void Shutdown(Action callback)
        {
            _forceShutDown = callback == null;
            if (!_forceShutDown && null != Interlocked.CompareExchange(ref _shutDownCallback, callback, null))
                throw new MemcacheException("Shutdown called twice on the same node: " + ToString());

            IMemcacheTransport transport;
            while (_transportPool.TryTake(out transport, 0))
                transport.Shutdown(_forceShutDown ? (Action)null : InternalShutdownCallback);
        }

        private void InternalShutdownCallback()
        {
            if (0 == Interlocked.Decrement(ref _workingTransports))
                _shutDownCallback();
        }

        internal bool IsClosing()
        {
            return _ongoingDispose || null != _shutDownCallback || _forceShutDown;
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
        internal int WorkingTransports { get { return _workingTransports; } }

        public override string ToString()
        {
            return "MemcacheNode " + EndPoint;
        }
    }
}
