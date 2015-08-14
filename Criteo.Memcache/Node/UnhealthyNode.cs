using System;
using System.Net;

using Criteo.Memcache.Configuration;

namespace Criteo.Memcache.Node
{
    /// <summary>
    /// An unhealthy node in a Couchbase cluster. It will not attempt any connection and fail all request.
    /// It should be replaced by a proper MemcacheNode as soon as the endpoint is healthy again.
    /// </summary>
    internal class UnhealthyNode : IMemcacheNode
    {
        public UnhealthyNode(EndPoint endPoint)
        {
            EndPoint = endPoint;
        }

        public event Action<Exception> TransportError;

        public event Action<Headers.MemcacheResponseHeader, Requests.IMemcacheRequest> MemcacheError;

        public event Action<Headers.MemcacheResponseHeader, Requests.IMemcacheRequest> MemcacheResponse;

        public event Action<IMemcacheNode> NodeDead;

        public event Action<IMemcacheNode> NodeAlive;

        public System.Net.EndPoint EndPoint { get; private set; }

        public bool IsDead
        {
            get { return true; }
        }

        public bool TrySend(Requests.IMemcacheRequest request, int timeout)
        {
            return false;
        }

        public void Shutdown(Action shutdownCallback)
        {
            shutdownCallback();
        }

        public void Dispose()
        { }

        public override string ToString()
        {
            return "UnhealthyNode " + EndPoint;
        }
    }
}