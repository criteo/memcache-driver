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
﻿using NUnit.Framework;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.UTest.Tests.Requests
{
    [TestFixture]
    public class SaslPlainTextRequestTests
    {
        static readonly byte[] SASL_PLAIN_QUERY = new byte[] 
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
                };

        [Test]
        public void SaslPlainTextRequestOkTest()
        {
            Status status = Status.UnknownCommand;
            var request = new SaslPlainRequest
            {
                Callback = s => status = s,
                User = @"user",
                Password = @"password",
                Zone = @"zone",
            };

            var queryBuffer = request.GetQueryBuffer();
            CollectionAssert.AreEqual(SASL_PLAIN_QUERY, queryBuffer, "The sasl plain text query buffer is different of the expected one");

            var header = new MemcacheResponseHeader { Opcode = Opcode.StepAuth, Status = Status.NoError };
            Assert.DoesNotThrow(() => request.HandleResponse(header, null, null, null));
            Assert.AreEqual(Status.NoError, status);
        }

        [Test]
        public void SaslPlainTextRequestFailTest()
        {
            Status status = Status.UnknownCommand;
            var request = new SaslPlainRequest
            {
                Callback = s => status = s,
            };

            Assert.DoesNotThrow(() => request.Fail());
            Assert.AreEqual(Status.InternalError, status);
        }
    }
}
