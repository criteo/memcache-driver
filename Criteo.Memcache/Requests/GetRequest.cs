using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Requests
{
    internal class GetRequest : IMemcacheRequest
    {
        public string Key { get; set; }
        public Action<Status, byte[]> CallBack { get; set; }
        public uint RequestId { get; set; }
        protected virtual Opcode RequestOpcode { get { return Opcode.Get; } }
        public uint Flag { get; private set; }

        public byte[] GetQueryBuffer()
        {
            var keyAsBytes = UTF8Encoding.Default.GetBytes(Key);
            if (keyAsBytes.Length > ushort.MaxValue)
                throw new ArgumentException("The key is too long for the memcache binary protocol : " + Key);

            var requestHeader = new MemcacheRequestHeader(RequestOpcode)
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

        public void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message)
        {
            if (header.Status == Status.NoError)
            {
                if (extra == null || extra.Length == 0)
                    throw new MemcacheException("The get command flag is not present !");
                else if (extra.Length != 4)
                    throw new MemcacheException("The get command flag is wrong size !");
                Flag = extra.CopyToUInt(0);
            }
            if (CallBack != null)
                CallBack(header.Status, message);
        }

        public void Fail()
        {
            if (CallBack != null)
                CallBack(Status.InternalError, null);
        }

        public override string ToString()
        {
            return RequestOpcode.ToString() + ";Id=" + RequestId + ";Key=" + Key;
        }
    }
}
