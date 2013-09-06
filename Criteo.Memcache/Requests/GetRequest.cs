using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Requests
{
    internal class GetRequest : RedundantRequest, IMemcacheRequest
    {
        static private DateTime Epock = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public string Key { get; set; }
        public Action<Status, byte[]> CallBack { get; set; }
        public uint RequestId { get; set; }
        public virtual Opcode RequestOpcode { get; set; }
        public uint Flag { get; private set; }
        public TimeSpan Expire { get; set; }

        public GetRequest()
        {
            // set the default opcode to get
            RequestOpcode = Opcode.Get;
        }

        public byte[] GetQueryBuffer()
        {
            if (RequestOpcode != Opcode.Get && RequestOpcode != Opcode.GetK && RequestOpcode != Opcode.GAT)
                throw new MemcacheException("Get request only supports Get, GetK or GAT opcodes");

            var keyAsBytes = UTF8Encoding.Default.GetBytes(Key);
            if (keyAsBytes.Length > ushort.MaxValue)
                throw new ArgumentException("The key is too long for the memcache binary protocol : " + Key);

            int extraLength = RequestOpcode == Opcode.GAT ?
                sizeof(uint) : 0;

            var requestHeader = new MemcacheRequestHeader(RequestOpcode)
            {
                KeyLength = (ushort)keyAsBytes.Length,
                ExtraLength = (byte)extraLength,
                TotalBodyLength = (uint)(extraLength + keyAsBytes.Length),
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.SIZE + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer, 0);

            // in case of Get and Touch, post the new TTL in extra
            if (RequestOpcode == Opcode.GAT)
            {
                uint expire;
                if (Expire.CompareTo(TimeSpan.FromDays(30)) < 0)
                    expire = (uint)Expire.TotalSeconds;
                else
                    expire = (uint)(DateTime.UtcNow.Add(Expire) - Epock).TotalSeconds;

                buffer.CopyFrom(MemcacheRequestHeader.SIZE + sizeof(uint), expire);
            }

            keyAsBytes.CopyTo(buffer, extraLength + MemcacheRequestHeader.SIZE);

            return buffer;
        }

        public void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message)
        {
            if (header.Status == Status.NoError)
            {
                if (extra == null || extra.Length == 0)
                    throw new MemcacheException("The get command flag is not present !");
                else if (extra.Length != 4)
                    throw new MemcacheException("The get command flag is the wrong size !");
                Flag = extra.CopyToUInt(0);
            }

            if (CallCallback(header.Status) && CallBack != null)
                CallBack(header.Status, message);
        }

        public void Fail()
        {
            if (CallCallback(Status.InternalError) && CallBack != null)
                CallBack(Status.InternalError, null);
        }

        public override string ToString()
        {
            return RequestOpcode.ToString() + ";Id=" + RequestId + ";Key=" + Key;
        }
    }
}
