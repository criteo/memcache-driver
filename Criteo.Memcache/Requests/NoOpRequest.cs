using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    internal class NoOpRequest : IMemcacheRequest
    {
        public uint RequestId { get; set; }
        public string Key { get; set; }
        public Action<MemcacheResponseHeader> Callback { get; set; }

        public int Replicas 
        { 
            get { return 0; }
            set { return;  } 
        }

        public byte[] GetQueryBuffer()
        {
            var requestHeader = new MemcacheRequestHeader(Opcode.NoOp)
            {
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.SIZE];
            requestHeader.ToData(buffer, 0);

            return buffer;
        }

        public void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message)
        {
            if (Callback != null)
                Callback(header);
        }

        public void Fail()
        {
            if (Callback != null)
                Callback(new MemcacheResponseHeader { Opcode = Opcode.NoOp, Status = Status.InternalError, Opaque = RequestId });
        }
    }
}
