using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Requests;

namespace Criteo.Memcache.Transport
{
    public interface ISynchronousTransport : IMemcacheTransport
    {
        bool TrySend(IMemcacheRequest req);
        void SetupAction(Action<ISynchronousTransport> action);
    }
}
