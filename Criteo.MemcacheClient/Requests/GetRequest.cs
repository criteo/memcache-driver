using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

            var requestHeader = new MemacheRequestHeader(Opcode.Get)
            {
                KeyLength = (ushort)keyAsBytes.Length,
                ExtraLength = 0,
                TotalBodyLength = (uint)(keyAsBytes.Length),
                Opaque = RequestId,
            };

            var buffer = new byte[24 + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer, 0);
            keyAsBytes.CopyTo(buffer, 24);

            return buffer;
        }

        public void HandleResponse(MemacheResponseHeader header, byte[] extra, byte[] message)
        {
            if (extra == null || extra.Length == 0)
                throw new Exception("The get command Magic Number is not present !");
            else if (extra.Length != 4)
                throw new Exception("The get command Magic Number is wrong size !");
            else if (extra.CopyToUInt(0) != 0xdeadbeef)
                throw new Exception("The get command Magic Number is wrong !");
                Callback(header.Status, message);
        }
    }
}
