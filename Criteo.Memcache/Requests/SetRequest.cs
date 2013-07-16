using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    class SetRequest : RedundantRequest, IMemcacheRequest
    {
        static private DateTime Epock = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private const uint RawDataFlag = 0xfa52;

        public string Key { get; set; }
        public byte[] Message { get; set; }
        public TimeSpan Expire { get; set; }
        public uint RequestId { get; set; }
        public uint Flags { get; set; }
        public Opcode Code { get; set; }

        public Action<Status> CallBack { get; set; }

        public SetRequest()
        {
            Code = Opcode.Set;
            Flags = RawDataFlag;
        }

        public byte[] GetQueryBuffer()
        {
            var keyAsBytes = UTF8Encoding.Default.GetBytes(Key);
            if (keyAsBytes.Length > ushort.MaxValue)
                throw new ArgumentException("The key is too long for the memcache binary protocol : " + Key);

            var requestHeader = new MemcacheRequestHeader(Code)
            {
                KeyLength = (ushort)keyAsBytes.Length,
                ExtraLength = 8,
                TotalBodyLength = (uint)(8 + keyAsBytes.Length + (Message == null? 0 : Message.Length)),
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.SIZE + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer, 0);
            buffer.CopyFrom(MemcacheRequestHeader.SIZE, Flags);

            uint expire;
            if (Expire.CompareTo(TimeSpan.FromDays(30)) < 0)
                expire = (uint)Expire.TotalSeconds;
            else
                expire = (uint)(DateTime.UtcNow.Add(Expire) - Epock).TotalSeconds;

            buffer.CopyFrom(MemcacheRequestHeader.SIZE + sizeof(uint), expire);
            keyAsBytes.CopyTo(buffer, 32);
            if(Message != null)
                Message.CopyTo(buffer, 32 + keyAsBytes.Length);

            return buffer;
        }

        // nothing to do on set response
        public void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message)
        {
            if (CallCallback(header.Status) && CallBack != null)
                CallBack(header.Status);
        }

        public void Fail()
        {
            if (CallCallback(Status.InternalError) && CallBack != null)
                CallBack(Status.InternalError);
        }

        public override string ToString()
        {
            return Code.ToString() + ";Id=" + RequestId + ";Key=" + Key;
        }
    }
}
