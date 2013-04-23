using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Transport;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.UTest.Mocks
{
    internal class TransportMock : IMemcacheTransport
    {
        private bool _isDead;
        public bool IsDead 
        {
            get { return _isDead; }
            set 
            {
                if (_isDead && !value && _setup != null)
                    _setup(this);
                _isDead = value;
            }
        }
        private Action<IMemcacheTransport> _setup;
        public Action<IMemcacheTransport> Setup
        {
            set
            {
                if (value != null)
                {
                    _setup = value;
                    if (!_isDead)
                        _setup(this);
                }
            }
        }
        public MemcacheResponseHeader Response { private get; set; }
        public byte[] Extra { private get; set; }
        public byte[] Message { private get; set; }

        public bool TrySend(IMemcacheRequest req)
        {
            if (IsDead)
            {
                return false;
            }
            else
            {
                req.HandleResponse(Response, null, Extra, Message);
                return true;
            }
        }

        public void PlanSetup()
        {
            if (!IsDead && _setup != null)
                _setup(this);
        }

        public void Kill()
        {
        }

#pragma warning disable 67
        public event Action<Exception> TransportError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;
#pragma warning restore 67

        public void Dispose()
        {
        }
    }
}
