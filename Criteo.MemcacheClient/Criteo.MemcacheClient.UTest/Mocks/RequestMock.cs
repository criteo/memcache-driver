using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Requests;
using Criteo.MemcacheClient.Headers;

namespace Criteo.MemcacheClient.UTest.Mocks
{
    class RequestMock : IMemcacheRequest
    {
        public uint RequestId { get; set; }
        public string Key { get; set; }

        public byte[] QueryBuffer { get; set; }
        public byte[] GetQueryBuffer()
        {
            return QueryBuffer;
        }

        public byte[] Extra { get; set; }
        public byte[] Message { get; set; }
        public MemcacheResponseHeader ResponseHeader { get; set; }
        public void HandleResponse(MemcacheResponseHeader header, byte[] extra, byte[] message)
        {
            ResponseHeader = header;
            Extra = extra;
            Message = message;
        }
    }
}
