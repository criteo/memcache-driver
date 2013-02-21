using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    class DeleteRequest : IMemcacheRequest
    {
        public string Key { get; set; }
        public uint RequestId { get; set; }
        public Action<Status> CallBack { get; set; }

        public byte[] GetQueryBuffer()
        {
            var keyAsBytes = ASCIIEncoding.Default.GetBytes(Key);
            if (keyAsBytes.Length > ushort.MaxValue)
                throw new ArgumentException("The key is too long for the memcache binary protocol : " + Key);

            var requestHeader = new MemcacheRequestHeader(Opcode.Delete)
            {
                KeyLength = (ushort)keyAsBytes.Length,
                ExtraLength = 0,
                TotalBodyLength = (uint)(keyAsBytes.Length),
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.SIZE + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer, 0);
            keyAsBytes.CopyTo(buffer, MemcacheResponseHeader.SIZE);

            return buffer;
        }

        // nothing to do on set response
        public void HandleResponse(MemcacheResponseHeader header, byte[] extra, byte[] message)
        {
            if (CallBack != null)
                CallBack(header.Status);
        }

        public void Fail()
        {
            if (CallBack != null)
                CallBack(Status.InternalError);
        }
    }
}
