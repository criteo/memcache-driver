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
        private readonly IList<IMemcacheTransport> _transportList;
        private int _workingTransport;
        private bool _isAlive = false;
        private CancellationTokenSource _tokenSource;

        private static SynchronousTransportAllocator DefaultAllocator = 
            (endPoint, authenticator, queueTimeout, pendingLimit, setupAction)
                => new MemcacheSocket(endPoint, authenticator, queueTimeout, pendingLimit, setupAction, false);

        #region Events
        public event Action<Exception> TransportError
        {
            add 
            {
                foreach(var transport in _transportList)
                    transport.TransportError += value; 
            }
            remove
            {
                foreach (var transport in _transportList) 
                    transport.TransportError -= value;
            }
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError
        {
            add
            {

                foreach (var transport in _transportList)
                    transport.MemcacheError += value;
            }
            remove
            {
                foreach (var transport in _transportList)
                    transport.MemcacheError -= value;
            }
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse
        {
            add
            {
                foreach (var transport in _transportList) 
                    transport.MemcacheResponse += value;
            }
            remove
            {
                foreach (var transport in _transportList) 
                    transport.MemcacheResponse -= value;
            }
        }

        public event Action<IMemcacheNode> NodeDead;

        public event Action<IMemcacheNode> NodeAlive;
        #endregion Events

        private readonly EndPoint _endPoint;
        public EndPoint EndPoint
        {
            get { return _endPoint; }
        }

        public MemcacheNode(EndPoint endPoint, MemcacheClientConfiguration configuration)
        {
            _endPoint = endPoint;
            _transportList = new List<IMemcacheTransport>(configuration.PoolSize);
            _transportPool = new BlockingCollection<IMemcacheTransport>(new ConcurrentBag<IMemcacheTransport>());

            for (int i = 0; i < configuration.PoolSize; ++i)
            {
                var transport = (configuration.SynchronousTransportFactory ?? DefaultAllocator)
                                    (endPoint, 
                                    configuration.Authenticator, 
                                    configuration.TransportQueueTimeout, 
                                    configuration.TransportQueueLength,
                                    TransportAlive);
                _transportList.Add(transport);
                TransportAlive(transport);
            }
        }

        private void TransportAlive(IMemcacheTransport transport)
        {
            if (_tokenSource == null || _tokenSource.IsCancellationRequested)
                lock(this)
                    if (_tokenSource == null || _tokenSource.IsCancellationRequested)
                        _tokenSource = new CancellationTokenSource();
            Thread.MemoryBarrier();

            Interlocked.Increment(ref _workingTransport);
            if (!_isAlive)
            {
                _isAlive = true;
                Thread.MemoryBarrier();
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
                    catch(Exception)
                    {
                        // if anything happen, don't let a transport outside of the pool
                        _transportPool.Add(transport);
                        throw;
                    }

                    if (sent)
                    {
                        // the transport sent the message, return it in the pool
                        _transportPool.Add(transport);
                        return true;
                    }
                    else
                    {
                        // TODO : don't flag as dead right now, start a timer to check if all transport are dead for more than configuration.DeadTimeout seconds

                        // the current transport is not working
                        Interlocked.Decrement(ref _workingTransport);

                        // let the transport plan to add it in the pool when it will be up again
                        transport.PlanSetup();

                        // no more transport ? it's dead ! (don't flag dead before PlanSetup, it can synchronously increment _workingTransport)
                        if (0 == _workingTransport)
                        {
                            _isAlive = false;
                            _tokenSource.Cancel();
                            if (NodeDead != null)
                                NodeDead(this);
                        }
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
            foreach(var transport in _transportList)
                transport.Dispose();
        }
    }
}
