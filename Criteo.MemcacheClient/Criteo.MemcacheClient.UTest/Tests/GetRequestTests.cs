using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;

using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.UTest.Tests
{
    [TestFixture]
    public class GetRequestTests
    {
        static readonly byte[] GET_QUERY = 
        {
            0x80, 0x00, 0x00, 0x05,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x05,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x48, 0x65, 0x6c, 0x6c,
            0x6f,
        };

        static readonly byte[] GET_MAGIC = 
        {
            0xde, 0xad, 0xbe, 0xef,
        };

        static readonly byte[] GET_BAD_MAGIC = 
        {
            0xb, 0xad, 0xfe, 0xed,
        };

        static readonly byte[] GET_MESSAGE = 
        {
            0x57, 0x6f, 0x72, 0x6c,
            0x64,
        };

        [Test]
        public void GetRequestTest()
        {
            byte[] message = null;
            var request = new GetRequest { Key = @"Hello", RequestId = 0, Callback = (s, m) => message = m };

            var queryBuffer = request.GetQueryBuffer();

            var condition = queryBuffer
                .Zip(GET_QUERY, (a, b) => a == b)
                .All(a => a);
            Assert.IsTrue(condition, "The get query buffer is different of the expected one");

            var header = new MemacheResponseHeader { Opcode = Opcode.Get, Status = Status.NoError };
            Assert.Throws(typeof(Exception), () => request.HandleResponse(header, GET_BAD_MAGIC, GET_MESSAGE), "The get query doesn't detect bad magic");
            Assert.DoesNotThrow(() => request.HandleResponse(header, GET_MAGIC, GET_MESSAGE));

            Assert.IsTrue(condition, "The get query buffer is different of the expected one");
            Assert.AreSame(message, GET_MESSAGE);
        }
    }
}
