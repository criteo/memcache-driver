using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Headers;

namespace Criteo.MemcacheClient.Requests
{
    internal class NoOpRequest : IMemcacheRequest
    {
        public uint RequestId { get; set; }
        public string Key { get; set; }
        public Action<MemcacheResponseHeader> Callback { get; set; }

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

        public void HandleResponse(MemcacheResponseHeader header, byte[] extra, byte[] message)
        {
            if (Callback != null)
                Callback(header);
        }
    }
}
