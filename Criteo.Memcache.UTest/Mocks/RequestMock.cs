using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.UTest.Mocks
{
    class RequestMock : IMemcacheRequest
    {
        public uint RequestId { get; set; }
        public string Key { get; set; }

        public int Replicas
        {
            get { return 0; }
            private set { return; }
        }

        public void Sent(int sentRequests) { }

        public ManualResetEventSlim Mutex { get; private set; }

        public byte[] QueryBuffer { get; set; }
        public byte[] GetQueryBuffer()
        {
            return QueryBuffer;
        }

        public RequestMock()
        {
            Mutex = new ManualResetEventSlim(false);
        }

        public byte[] Extra { get; private set; }
        public byte[] Message { get; private set; }
        public MemcacheResponseHeader ResponseHeader { get; private set; }
        public void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message)
        {
            ResponseHeader = header;
            Extra = extra;
            Message = message;
            Mutex.Set();
        }

        public void Fail()
        {
            Mutex.Set();
        }
    }
}
