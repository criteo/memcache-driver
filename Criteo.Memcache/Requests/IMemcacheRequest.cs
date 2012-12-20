using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    public interface IMemcacheRequest
    {
        uint RequestId { get; set; }
        string Key { get; set; }
        
        byte[] GetQueryBuffer();
        void HandleResponse(MemcacheResponseHeader header, byte[] extra, byte[] message);
    }
}
