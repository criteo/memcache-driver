using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;

namespace Criteo.Memcache.UTest.Mocks
{
    internal class NodeMock : IMemcacheNode
    {
#pragma warning disable 67
        public event Action<Exception> TransportError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;
#pragma warning restore 67
        public event Action<IMemcacheNode> NodeDead;

        public System.Net.IPEndPoint EndPoint
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
            }
        }

        public IMemcacheRequest LastRequest { get; private set; }

        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            LastRequest = request;
            return true;
        }

        public void Dispose()
        {
        }
    }
}
