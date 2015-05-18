using System;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    class IncrementRequest : RedundantRequest, IRedundantRequest, ICouchbaseRequest
    {
        private TimeSpan _expire;

        public Opcode RequestOpcode
        {
            get;
            set;
        }
        public Action<Status, byte[]> CallBack { get; set; }
        public ulong Delta { get; set; }
        public ulong Initial { get; set; }
        public TimeSpan Expire
        {
            get
            {
                return _expire;
            }

            set
            {
                if (!ExpirationTimeUtils.IsValid(value))
                    throw new ArgumentException("Invalid expiration time: " + value.ToString());

                _expire = value;
            }
        }

        public IncrementRequest()
        {
            RequestOpcode = Opcode.Increment;
        }

        public byte[] GetQueryBuffer()
        {
            const int extraLength = sizeof(ulong) + sizeof(ulong) + sizeof(uint);

            var requestHeader = new MemcacheRequestHeader(RequestOpcode)
            {
                VBucket = VBucket,
                KeyLength = (ushort)Key.Length,
                ExtraLength = (byte)extraLength,
                TotalBodyLength = (uint)(extraLength + Key.Length),
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.Size + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer);
            buffer.CopyFrom(MemcacheRequestHeader.Size, Delta);
            buffer.CopyFrom(MemcacheRequestHeader.Size + sizeof(ulong), Initial);
            buffer.CopyFrom(MemcacheRequestHeader.Size + 2 * sizeof(ulong), ExpirationTimeUtils.MemcachedTTL(Expire));

            Key.CopyTo(buffer, extraLength + MemcacheRequestHeader.Size);

            return buffer;
        }

        public void HandleResponse(MemcacheResponseHeader header, byte[] key, byte[] extra, byte[] message)
        {
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
