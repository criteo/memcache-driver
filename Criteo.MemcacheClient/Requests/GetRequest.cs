using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.MemcacheClient.Requests
{
    internal class GetRequest : IMemcacheRequest
    {
        public string Key { get; set; }
        public Action<byte[]> Callback { get; set; }
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

        public void HandleResponse(MemacheResponseHeader header, byte[] message)
        {
            if (message.CopyToUInt(0) != 0xdeadbeef)
                throw new Exception("The get command Magic Number is wrong !");
            var result = new byte[message.Length - 4];
            Array.Copy(message, 4, result, 0, result.Length);
            if (header.Status == Status.NoError)
                Callback(result);
        }
    }
}
