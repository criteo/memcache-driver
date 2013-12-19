using System;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.UTest.Mocks
{
    internal class NodeMock : IMemcacheNode
    {
#pragma warning disable 67
        public event Action<Exception> TransportError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;
        public event Action<IMemcacheNode> NodeAlive;
#pragma warning restore 67
        public event Action<IMemcacheNode> NodeDead;

        public System.Net.EndPoint EndPoint
        {
            get;
            set;
        }

        private bool _isDead;
        public bool IsDead
        {
            get { return _isDead; }
            set
            {
                _isDead = value;
                if (_isDead && NodeDead != null)
                    NodeDead(this);
                if (!_isDead && NodeDead != null)
                    NodeAlive(this);
            }
        }

        public Status DefaultResponse { get; set; }

        public IMemcacheRequest LastRequest { get; private set; }

        public static int trySendCounter;

        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            LastRequest = request;
            trySendCounter++;

            if (timeout == 0)
            {
                LastRequest.HandleResponse(new MemcacheResponseHeader { Status = DefaultResponse, ExtraLength = 4 }, request.Key, new byte[4], new byte[0]);
            }

            return true;
        }

        public void Dispose()
        {
        }
    }
}
