using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Collections.Concurrent;
using System.Threading;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Transport;

namespace Criteo.Memcache.Node
{
    internal class MemcacheNode : IMemcacheNode
    {
        private readonly BlockingCollection<IMemcacheTransport> _transportPool;
        private int _workingTransport;
        private volatile bool _isAlive = false;
        private volatile CancellationTokenSource _tokenSource;
        private MemcacheClientConfiguration _configuration;

        private static SynchronousTransportAllocator DefaultAllocator = 
            (endPoint, authenticator, queueTimeout, pendingLimit, setupAction, autoConnect)
                => new MemcacheSocket(endPoint, authenticator, queueTimeout, pendingLimit, setupAction, false, autoConnect);

        #region Events
        private void OnTransportError(Exception e)
        {
            if (TransportError != null)
                TransportError(e);
        }
        public event Action<Exception> TransportError;

        private void OnMemcacheError(MemcacheResponseHeader h, IMemcacheRequest e)
        {
            if (MemcacheError != null)
                MemcacheError(h, e);
        }
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;

        private void OnMemcacheResponse(MemcacheResponseHeader h, IMemcacheRequest e)
        {
            if (MemcacheResponse != null)
                MemcacheResponse(h, e);
        }
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;

        public event Action<IMemcacheNode> NodeDead;

        public event Action<IMemcacheNode> NodeAlive;

        private void RegisterEvents(IMemcacheTransport transport)
        {
            transport.MemcacheError += OnMemcacheError;
            transport.MemcacheResponse += OnMemcacheResponse;
            transport.TransportError += OnTransportError;
        }

        private void UnregisterEvents(IMemcacheTransport transport)
        {
            transport.MemcacheError -= OnMemcacheError;
            transport.MemcacheResponse -= OnMemcacheResponse;
            transport.TransportError -= OnTransportError;
        }
        #endregion Events

        private readonly EndPoint _endPoint;
        public EndPoint EndPoint
        {
            get { return _endPoint; }
        }

        public MemcacheNode(EndPoint endPoint, MemcacheClientConfiguration configuration)
        {
            _configuration = configuration;
            _endPoint = endPoint;
            _transportPool = new BlockingCollection<IMemcacheTransport>(new ConcurrentBag<IMemcacheTransport>());

            for (int i = 0; i < configuration.PoolSize; ++i)
            {
                var transport = (configuration.SynchronousTransportFactory ?? DefaultAllocator)
                                    (endPoint, 
                                    configuration.Authenticator, 
                                    configuration.TransportQueueTimeout, 
                                    configuration.TransportQueueLength,
                                    TransportAlive,
                                    false);
                RegisterEvents(transport);
                TransportAlive(transport);
            }
        }


        private void TransportAlive(IMemcacheTransport transport)
        {
            if (_tokenSource == null || _tokenSource.IsCancellationRequested)
                lock (this)
                    if (_tokenSource == null || _tokenSource.IsCancellationRequested)
                        _tokenSource = new CancellationTokenSource();

            Interlocked.Increment(ref _workingTransport);
            if (!_isAlive)
                lock (this)
                    if (!_isAlive)
                    {
                        _isAlive = true;
                        if (NodeAlive != null)
                            NodeAlive(this);
                    }

            _transportPool.Add(transport);
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
                while (_tokenSource != null && !_tokenSource.IsCancellationRequested
                    && _transportPool.TryTake(out transport, timeout, _tokenSource.Token))
                {
                    bool sent = false;
                    try
                    {
                        sent = transport.TrySend(request);
                    }
                    catch (Exception)
                    {
                        // if anything happen, don't let a transport outside of the pool
                        _transportPool.Add(transport);
                        throw;
                    }

                    if (transport.IsAlive)
                    {
                        // the transport sent the message, return it in the pool
                        _transportPool.Add(transport);
                        return sent;
                    }
                    else
                    {
                        // TODO : don't flag as dead right now, start a timer to check if all transport are dead for more than configuration.DeadTimeout seconds

                        lock (this)
                        {
                            // the current transport is not working
                            var workingCount = Interlocked.Decrement(ref _workingTransport);

                            // no more transport ? it's dead ! (don't flag dead before PlanSetup, it can synchronously increment _workingTransport)
                            if (0 == workingCount)
                            {
                                _isAlive = false;
                                _tokenSource.Cancel();
                                if (NodeDead != null)
                                    NodeDead(this);
                            }
                        }

                        // shoot the dead transport
                        UnregisterEvents(transport);
                        transport.Dispose();

                        // then create a fresh new one
                        var newTransport = (_configuration.SynchronousTransportFactory ?? DefaultAllocator)
                                    (_endPoint,
                                    _configuration.Authenticator,
                                    _configuration.TransportQueueTimeout,
                                    _configuration.TransportQueueLength,
                                    TransportAlive,
                                    true);
                        RegisterEvents(newTransport);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // someone called for a cancel on the pool.TryTake and already raised the problem
                return false;
            }

            return false;
        }

        public void Dispose()
        {
            IMemcacheTransport transport;
            while(_transportPool.TryTake(out transport))
                transport.Dispose();
        }

        // for testing purpose only !!!
        internal int PoolSize { get { return _transportPool.Count; } }
    }
}
