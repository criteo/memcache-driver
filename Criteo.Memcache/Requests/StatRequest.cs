using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    internal class StatRequest : IMemcacheRequest
    {
        public uint RequestId { get; set; }
        public string Key { get; set; }
        public Action<IDictionary<string, string>> Callback;

        public int Replicas
        {
            get { return 0; }
            set { return; }
        }

        private Dictionary<string, string> _result = null;

        public byte[] GetQueryBuffer()
        {
            var keyAsBytes = Key == null ? null : UTF8Encoding.Default.GetBytes(Key);
            var message = new byte[MemcacheRequestHeader.SIZE + (Key == null ? 0 : keyAsBytes.Length)];
            new MemcacheRequestHeader(Opcode.Stat)
            {
                Opaque = RequestId,
                KeyLength = (ushort)(Key == null ? 0 : keyAsBytes.Length),
            }.ToData(message);

            if(keyAsBytes != null)
                keyAsBytes.CopyTo(message, MemcacheRequestHeader.SIZE);

            return message;
        }

        public void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message)
        {
            if (key != null)
            {
                if (_result == null)
                    _result = new Dictionary<string, string>();
                _result.Add(key, message != null ? UTF8Encoding.Default.GetString(message) : null);
            }
            else if (Callback != null)
                Callback(_result);
        }

        public void Fail()
        {
            Callback(_result);
        }
    }
}
