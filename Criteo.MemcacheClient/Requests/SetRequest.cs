using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Criteo.MemcacheClient.Headers;

namespace Criteo.MemcacheClient.Requests
{
    class SetRequest : IMemcacheRequest
    {
        public string Key { get; set; }
        public byte[] Message { get; set; }
        public TimeSpan Expire { get; set; }
        public uint RequestId { get; set; }

        public byte[] GetQueryBuffer()
        {
            var keyAsBytes = ASCIIEncoding.Default.GetBytes(Key);
            if (keyAsBytes.Length > ushort.MaxValue)
                throw new ArgumentException("The key is too long for the memcache binary protocol : " + Key);

            var requestHeader = new MemacheRequestHeader(Opcode.Set)
            {
                KeyLength = (ushort)keyAsBytes.Length,
                ExtraLength = 8,
                TotalBodyLength = (uint)(8 + keyAsBytes.Length + Message.Length),
                Opaque = RequestId,
            };

            var buffer = new byte[24 + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer, 0);
            buffer.CopyFrom(24, (uint)0xdeadbeef);
            buffer.CopyFrom(28, (uint)Expire.TotalSeconds);
            keyAsBytes.CopyTo(buffer, 32);
            Message.CopyTo(buffer, 32 + keyAsBytes.Length);

            return buffer;
        }

        // nothing to do on set response
        public void HandleResponse(MemacheResponseHeader header, byte[] extra, byte[] message)
        {
        }
    }
}
