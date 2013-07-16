using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    /// <summary>
    /// Interface implemented by the requests
    /// </summary>
    public interface IMemcacheRequest
    {
        uint RequestId { get; set; }
        string Key { get; set; }
        int Replicas { get; }

        byte[] GetQueryBuffer();
        void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message);
        void Fail();
    }
}
