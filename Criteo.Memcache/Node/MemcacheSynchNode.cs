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
    internal class MemcacheSynchNode : IMemcacheNode
    {
        private readonly BlockingCollection<ISynchronousTransport> _transportPool;
        private readonly IList<ISynchronousTransport> _transportList;
        private int _workingTransport;
        private CancellationTokenSource _tokenSource;

        private static SynchornousTransportAllocator DefaultAllocator = 
            (endPoint, authenticator, node, queueTimeout, pendingLimit, setupAction)
                => new MemcacheSocketSynchronous(endPoint, authenticator, node, queueTimeout, pendingLimit, setupAction, false);

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
        #endregion Events

        public event Action<IMemcacheNode> NodeDead;

        private IPEndPoint _endPoint;
        public IPEndPoint EndPoint
        {
            get { return _endPoint; }
        }

        public MemcacheSynchNode(IPEndPoint endPoint, MemcacheClientConfiguration configuration, Action<IMemcacheRequest> requeueRequest)
        {
            _endPoint = endPoint;
            _transportList = new List<ISynchronousTransport>(configuration.PoolSize);
            _transportPool = new BlockingCollection<ISynchronousTransport>(new ConcurrentBag<ISynchronousTransport>());

            for (int i = 0; i < configuration.PoolSize; ++i)
            {
                var transport = (configuration.SynchornousTransportFactory ?? DefaultAllocator)
                                    (endPoint, 
                                    configuration.Authenticator, 
                                    null,
                                    configuration.TransportQueueTimeout, 
                                    configuration.TransportQueueLength,
                                    TransportAlive);
                _transportList.Add(transport);
            }
        }

        private void TransportAlive(ISynchronousTransport transport)
        {
            _transportPool.Add(transport);
            Interlocked.Increment(ref _workingTransport);

            if (_tokenSource == null || _tokenSource.IsCancellationRequested)
                _tokenSource = new CancellationTokenSource();
        }

        public bool IsDead
        {
            get { return _workingTransport == 0; }
        }

        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            ISynchronousTransport transport;
            while (_tokenSource != null && !_tokenSource.IsCancellationRequested
                && _transportPool.TryTake(out transport, timeout, _tokenSource.Token))
            {
                if (transport.TrySend(request))
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

                    // no more transport ? it's dead ! (don't flag dead before SetupAction, it can synchronously increment _workingTransport)
                    if (0 == _workingTransport)
                    {
                        _tokenSource.Cancel();
                        if (NodeDead != null)
                            NodeDead(this);
                    }
                }
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
