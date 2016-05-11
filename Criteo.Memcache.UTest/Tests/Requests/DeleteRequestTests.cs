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

using System.Linq;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;

using NUnit.Framework;

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
            var request = new DeleteRequest(CallBackPolicy.AllOK)
            {
                Key = "Hello".Select(c => (byte)c).ToArray(),
                RequestId = 0, 
                CallBack = (s) => status = s,
            };

            var queryBuffer = request.GetQueryBuffer();
            Assert.IsNotNull(queryBuffer);
            CollectionAssert.AreEqual(DELETE_QUERY, queryBuffer, "The delete query buffer is different of the expected one");

            var header = new MemcacheResponseHeader { Opcode = Opcode.Delete, Status = Status.NoError };
            Assert.DoesNotThrow(() => request.HandleResponse(header, null, null, null));
            Assert.AreEqual(Status.NoError, status);
        }

        [Test]
        public void DeleteRequestFailTest()
        {
            Status status = Status.UnknownCommand;
            var request = new DeleteRequest(CallBackPolicy.AllOK)
            {
                Key = "Hello".Select(c => (byte)c).ToArray(),
                RequestId = 0,
                CallBack = (s) => status = s,
            };

            Assert.DoesNotThrow(request.Fail);
            Assert.AreEqual(Status.InternalError, status, "The status sent by a fail should be InternalError");
        }
    }
}
