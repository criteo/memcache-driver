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
        private bool _isAlive;
        public bool IsAlive
        {
            get { return _isAlive; }
            set 
            {
                if (!_isAlive && value && Setup != null)
                    Setup(this);

                if (_isAlive && !value && TransportDead != null)
                {
                    TransportDead(this);

                    TransportError = null;
                    MemcacheError = null;
                    MemcacheResponse = null;
                    TransportDead = null;
                }

                _isAlive = value;
            }
        }
        public Action<IMemcacheTransport> Setup;
        public MemcacheResponseHeader Response { private get; set; }
        public byte[] Extra { private get; set; }
        public byte[] Message { private get; set; }

        public bool TrySend(IMemcacheRequest req)
        {
            if (!IsAlive)
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
            if (IsAlive && Setup != null)
                Setup(this);
        }

        public void Kill()
        {
        }

#pragma warning disable 67
        public event Action<Exception> TransportError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;
        public event Action<IMemcacheTransport> TransportDead;
#pragma warning restore 67

        public void Dispose()
        {
        }

        public bool Registered
        {
            get { return TransportDead != null; }
        }
    }
}
