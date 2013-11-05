using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;
using Criteo.Memcache.Transport;

namespace Criteo.Memcache.UTest
{
    internal class MemcacheTransportForTest : MemcacheTransport
    {
        public Action OnDispose { private get; set; }

        public MemcacheTransportForTest(EndPoint endpoint, MemcacheClientConfiguration clientConfig, Action<IMemcacheTransport> registerEvents, Action<IMemcacheTransport> transportAvailable, bool planToConnect, IOngoingDispose nodeDispose, Action onCreate, Action onDispose)
            : base(endpoint, clientConfig, registerEvents, transportAvailable, planToConnect, nodeDispose)
        {
            if (onCreate != null)
                onCreate();

            OnDispose = onDispose;
        }

        // Hiding MemcacheTransport.Dispose()
        public override void Dispose()
        {
            if(OnDispose != null)
                OnDispose();
            base.Dispose();
        }

        public override string ToString()
        {
            return "MemcacheTransportForTest " + GetHashCode();
        }
    }
}
