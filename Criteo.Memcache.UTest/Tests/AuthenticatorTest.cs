using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;

using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    class AuthenticatorTest
    {
        [Test]
        public void SaslPlainTextAuthicatorNoErrorTest()
        {
            var authenticator = new SaslPlainTextAuthenticator
            {
                User = @"user",
                Password = @"password",
                Zone = @"zone",
            };

            var token = authenticator.CreateToken();
            Assert.IsNotNull(token);

            IMemcacheRequest request;
            // initial step, nothing done, it must say we have to authenticate
            Assert.AreEqual(Status.StepRequired, token.StepAuthenticate(out request));
            Assert.IsNotNull(request);

            // checks it builds the right message
            var buffer = request.GetQueryBuffer();
            Assert.IsNotNull(buffer);
            CollectionAssert.AreEqual(new byte[]
                {
                // magic, opcode, key length ("PLAIN")
                    0x80, 0x21, 0x00, 0x05,
                // extra legth, data type, reserved
                    0x00, 0x00, 0x00, 0x00,
                // total body length (key + user + password + zone + 2 separators)
                    0x00, 0x00, 0x00, 0x17,
                // opaque (RequestId)
                    0x00, 0x00, 0x00, 0x00,
                // CAS
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // "PLAIN"
                    0x50, 0x4C, 0x41, 0x49, 0x4E,
                // "zone" + \0 + "user" + \0 + "password"
                    0x7A, 0x6F, 0x6E, 0x65, 0x00, 0x75, 0x73, 0x65, 0x72, 0x00, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6F, 0x72, 0x64
                }, buffer);

            // fake a call of the request
            request.HandleResponse(new MemcacheResponseHeader { Status = Status.NoError }, null, null, null);

            // check it works
            Assert.AreEqual(Status.NoError, token.StepAuthenticate(out request));
            Assert.IsNull(request);
        }

        [Test]
        public void SaslPlainTextAuthicatorErrorTest()
        {
            var authenticator = new SaslPlainTextAuthenticator
            {
                User = @"user",
                Password = @"password",
                Zone = @"zone",
            };

            var token = authenticator.CreateToken();
            Assert.IsNotNull(token);

            IMemcacheRequest request;
            // initial step, nothing done, it must say we have to authenticate
            Assert.AreEqual(Status.StepRequired, token.StepAuthenticate(out request));
            Assert.IsNotNull(request);

            var buffer = request.GetQueryBuffer();
            Assert.IsNotNull(buffer);

            // fake a call of the request
            request.HandleResponse(new MemcacheResponseHeader { Status = Status.StepRequired }, null, null, null);

            // check it returns the sent status
            Assert.AreEqual(Status.StepRequired, token.StepAuthenticate(out request));
        }
    }
}
