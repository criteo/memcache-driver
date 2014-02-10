/* Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/
using NUnit.Framework;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.UTest.Tests.Requests
{
    [TestFixture]
    public class NoOpRequestTests
    {
        static readonly byte[] NOOP_QUERY = new byte[] 
                {   
                // magic, opcode, key length
                    0x80, 0x0a, 0x00, 0x00,
                // extra legth, data type, reserved
                    0x00, 0x00, 0x00, 0x00, 
                // total body length
                    0x00, 0x00, 0x00, 0x00,
                // opaque (RequestId)
                    0x00, 0x00, 0x00, 0x00, 
                // CAS
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                };

        [Test]
        public void NoOpRequestOkTest()
        {
            Status status = Status.UnknownCommand;
            var request = new NoOpRequest
            {
                Callback = (h) => status = h.Status,
            };

            var queryBuffer = request.GetQueryBuffer();
            Assert.IsNotNull(queryBuffer);
            CollectionAssert.AreEqual(NOOP_QUERY, queryBuffer, "The noop query buffer is different of the expected one");

            var header = new MemcacheResponseHeader { Opcode = Opcode.NoOp, Status = Status.NoError };
            Assert.DoesNotThrow(() => request.HandleResponse(header, null, null, null));
            Assert.AreEqual(Status.NoError, status);
        }

        [Test]
        public void NoOpRequestFailTest()
        {
            Status status = Status.UnknownCommand;
            var request = new NoOpRequest
            {
                Callback = (h) => status = h.Status,
            };

            var queryBuffer = request.GetQueryBuffer();

            var header = new MemcacheResponseHeader { Opcode = Opcode.NoOp, Status = Status.NoError };
            Assert.DoesNotThrow(() => request.Fail());
            Assert.AreEqual(Status.InternalError, status);
        }
    }
}
