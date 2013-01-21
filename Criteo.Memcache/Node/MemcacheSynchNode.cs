using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Transport;

namespace Criteo.Memcache.Node
{
    /// <summary>
    /// Experimental synchronous sends on a single socket
    /// Untested, use it at your own risks !
    /// </summary>
    internal class MemcacheSynchNode : IMemcacheNode
    {
        private static SocketAllocator DefaultAllocator = (endPoint, authenticator, mutex) => new MemcacheSocketSynchronous(endPoint, authenticator, mutex);

        public event Action<Exception> TransportError
        {
            add { _transport.TransportError += value; }
            remove { _transport.TransportError -= value; }
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError
        {
            add { _transport.MemcacheError += value; }
            remove { _transport.MemcacheError -= value; }
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse
        {
            add { _transport.MemcacheResponse += value; }
            remove { _transport.MemcacheResponse -= value; }
        }

        public event Action<IMemcacheNode> NodeDead;

        private IPEndPoint _endPoint;
        public IPEndPoint EndPoint
        {
            get { return _endPoint; }
        }

        private object _mutex = new object();

        private IMemcacheTransport _transport;
        public MemcacheSynchNode(IPEndPoint endPoint, MemcacheClientConfiguration configuration, Action<IMemcacheRequest> requeueRequest)
        {
            _endPoint = endPoint;
            _transport = (configuration.SocketFactory ?? DefaultAllocator)(endPoint, null, configuration.Authenticator);
        }

        public bool IsDead
        {
            get { return false; }
        }

        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            lock (_mutex)
                _transport.RequestsQueue.Add(request);

            return true;
        }

        public void Dispose()
        {
            _transport.Dispose();
        }
    }
}
