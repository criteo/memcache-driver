using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Requests
{
    internal class SaslPlainRequest : IMemcacheRequest
    {
        public uint RequestId
        {
            get { return 0; }
            set { return; }
        }

        public string Key
        {
            get { return "PLAIN"; }
            set { return; }
        }

        public string Zone { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public Action<Status> Callback { get; set; }

        public byte[] GetQueryBuffer()
        {
            var key = ASCIIEncoding.Default.GetBytes(Key);
            var data = Encoding.UTF8.GetBytes(Zone + "\0" + User + "\0" + Password);

            var header = new MemcacheRequestHeader(Opcode.StartAuth)
            {
                ExtraLength = 0,
                TotalBodyLength = (uint)(key.Length + data.Length),
            };

            var message = new byte[MemcacheRequestHeader.SIZE + header.TotalBodyLength];
            header.ToData(message);
            Array.Copy(key, 0, message, MemcacheRequestHeader.SIZE, key.Length);
            Array.Copy(data, 0, message, MemcacheRequestHeader.SIZE + key.Length, message.Length);

            return message;
        }

        public void HandleResponse(MemcacheResponseHeader header, byte[] extra, byte[] message)
        {
            if (Callback != null)
                Callback(header.Status);
        }
    }
}
