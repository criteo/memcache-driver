using System;
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

        private static TransportAllocator DefaultAllocator = 
            (endPoint, authenticator, queueTimeout, pendingLimit, setupAction, autoConnect)
                => new MemcacheTransport(endPoint, authenticator, queueTimeout, pendingLimit, setupAction, autoConnect);

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

        private void RegisterEvents(IMemcacheTransport transport)
        {
            transport.MemcacheError += OnMemcacheError;
            transport.MemcacheResponse += OnMemcacheResponse;
            transport.TransportError += OnTransportError;
            transport.TransportDead += OnTransportDead;
        }

        private void OnTransportDead(IMemcacheTransport transort)
        {
            lock (this)
            {
                if (0 == Interlocked.Decrement(ref _workingTransport))
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
            _configuration = configuration;
            _endPoint = endPoint;
            _tokenSource = new CancellationTokenSource();
            _transportPool = new BlockingCollection<IMemcacheTransport>(new ConcurrentQueue<IMemcacheTransport>());

            for (int i = 0; i < configuration.PoolSize; ++i)
            {
                var transport = (configuration.TransportFactory ?? DefaultAllocator)
                                    (endPoint, 
                                    configuration.Authenticator, 
                                    configuration.TransportQueueTimeout, 
                                    configuration.TransportQueueLength,
                                    TransportAvailable,
                                    false);
                TransportAvailable(transport);
            }
        }

        private void TransportAvailable(IMemcacheTransport transport)
        {
            if (!transport.Registered)
            {
                RegisterEvents(transport);

                Interlocked.Increment(ref _workingTransport);
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
                int tries = 0;
                while (!_tokenSource.IsCancellationRequested
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

        public void Dispose()
        {
            IMemcacheTransport transport;
            while(_transportPool.TryTake(out transport))
                transport.Dispose();
        }

        // for testing purpose only !!!
        internal int PoolSize { get { return _transportPool.Count; } }

        public override string ToString()
        {
            return "MemcacheNode " + EndPoint;
        }
    }
}
