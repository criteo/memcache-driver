using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.UTest.Tests.Requests
{
    [TestFixture]
    public class DeleteRequestTests
    {
        static readonly byte[] DELETE_QUERY = new byte[] 
                {   
                // magic, opcode, key length ("Hello")
                    0x80, 0x04, 0x00, 0x05,
                // extra legth, data type, reserved
                    0x00, 0x00, 0x00, 0x00, 
                // total body length (key + user + password + zone + 2 separators)
                    0x00, 0x00, 0x00, 0x05,
                // opaque (RequestId)
                    0x00, 0x00, 0x00, 0x00, 
                // CAS
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // "Hello"
                    0x48, 0x65, 0x6c, 0x6c, 0x6f
                };

        [TestCase]
        public void DeleteRequestTest()
        {
            Status status = Status.UnknownCommand;
            var request = new DeleteRequest 
            {
                Key = @"Hello", 
                RequestId = 0, 
                CallBack = (s) => status = s,
            };

            var queryBuffer = request.GetQueryBuffer();
            Assert.IsNotNull(queryBuffer);
            CollectionAssert.AreEqual(DELETE_QUERY, queryBuffer, "The delete query buffer is different of the expected one");

            var header = new MemcacheResponseHeader { Opcode = Opcode.Delete, Status = Status.NoError };
            Assert.DoesNotThrow(() => request.HandleResponse(header, null, null));
            Assert.AreEqual(Status.NoError, status);
        }

        [Test]
        public void DeleteRequestFailTest()
        {
            Status status = Status.UnknownCommand;
            var request = new DeleteRequest
            {
                Key = @"Hello",
                RequestId = 0,
                CallBack = (s) => status = s,
            };

            Assert.DoesNotThrow(() => request.Fail());
            Assert.AreEqual(Status.InternalError, status, "The status sent by a fail should be InternalError");
        }
    }
}
