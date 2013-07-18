using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class SetRequestTests
    {
        static readonly byte[] SET_QUERY = 
        {
            0x80, 0x01, 0x00, 0x05,
            0x08, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x12,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0xfa, 0x52,
            0x00, 0x00, 0x0e, 0x10,
            0x48, 0x65, 0x6c, 0x6c,
            0x6f, 0x57, 0x6f, 0x72,
            0x6c, 0x64,
        };

        static readonly byte[] SET_EXTRA = null;

        static readonly byte[] SET_MESSAGE = null;

        /// <summary>
        /// Test around the GetRequest object
        /// </summary>
        [Test]
        public void SetRequestTest()
        {
            Status status = Status.UnknownCommand;
            var request = new SetRequest 
            { 
                Key = @"Hello",
                Message = System.Text.UTF8Encoding.Default.GetBytes(@"World"), 
                RequestId = 0, 
                Expire = TimeSpan.FromHours(1),
                CallBack = (s) => status = s,
            };

            Assert.AreEqual(Opcode.Set, request.Code);

            var queryBuffer = request.GetQueryBuffer();
            CollectionAssert.AreEqual(SET_QUERY, queryBuffer, "The set query buffer is different of the expected one");

            var header = new MemcacheResponseHeader { Opcode = Opcode.Set, Status = Status.NoError };
            Assert.DoesNotThrow(() => request.HandleResponse(header, null, SET_EXTRA, SET_MESSAGE));
            Assert.AreEqual(Status.NoError, status);
        }

        [Test]
        public void SetRequestFailTest()
        {
            Status status = Status.UnknownCommand;
            var request = new SetRequest
            {
                Key = @"Hello",
                Message = System.Text.UTF8Encoding.Default.GetBytes(@"World"),
                RequestId = 0,
                Expire = TimeSpan.FromHours(1),
                CallBack = (s) => status = s,
            };

            var queryBuffer = request.GetQueryBuffer();
            CollectionAssert.AreEqual(SET_QUERY, queryBuffer, "The set query buffer is different of the expected one");

            Assert.DoesNotThrow(() => request.Fail());
            Assert.AreEqual(Status.InternalError, status, "The status sent by a fail should be InternalError");
        }
    }
}
