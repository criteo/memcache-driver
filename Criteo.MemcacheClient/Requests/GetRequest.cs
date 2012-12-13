using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Headers;
using Criteo.MemcacheClient.Exceptions;

namespace Criteo.MemcacheClient.Requests
{
    internal class GetRequest : IMemcacheRequest
    {
        public string Key { get; set; }
        public Action<Status, byte[]> Callback { get; set; }
        public uint RequestId { get; set; }
        protected virtual Opcode RequestOpcode { get { return Opcode.Get; } }

        public byte[] GetQueryBuffer()
        {
            var keyAsBytes = ASCIIEncoding.Default.GetBytes(Key);
            if (keyAsBytes.Length > ushort.MaxValue)
                throw new ArgumentException("The key is too long for the memcache binary protocol : " + Key);

            var requestHeader = new MemcacheRequestHeader(Opcode.Get)
            {
                KeyLength = (ushort)keyAsBytes.Length,
                ExtraLength = 0,
                TotalBodyLength = (uint)(keyAsBytes.Length),
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.SIZE + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer, 0);
            keyAsBytes.CopyTo(buffer, MemcacheRequestHeader.SIZE);

            return buffer;
        }

        public void HandleResponse(MemcacheResponseHeader header, byte[] extra, byte[] message)
        {
            if (extra == null || extra.Length == 0)
                throw new MemcacheException("The get command Magic Number is not present !");
            else if (extra.Length != 4)
                throw new MemcacheException("The get command Magic Number is wrong size !");
            else if (extra.CopyToUInt(0) != 0xdeadbeef)
                throw new MemcacheException("The get command Magic Number is wrong !");
                Callback(header.Status, message);
        }
    }
}
