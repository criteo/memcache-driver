using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.MemcacheClient.Requests
{
    public class NoOpRequest : IMemcacheRequest
    {
        public uint RequestId { get; set; }
        public string Key { get; set; }
        public Action<MemacheResponseHeader> Callback { get; set; }

        public byte[] GetQueryBuffer()
        {
            var requestHeader = new MemacheRequestHeader(Opcode.NoOp)
            {
                Opaque = RequestId,
            };

            var buffer = new byte[24];
            requestHeader.ToData(buffer, 0);

            return buffer;
        }

        public void HandleResponse(MemacheResponseHeader header, byte[] extra, byte[] message)
        {
            if (Callback != null)
                Callback(header);
        }
    }
}
