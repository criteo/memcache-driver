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
﻿using System;

using NUnit.Framework;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;

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
                CallBackPolicy = CallBackPolicy.AllOK
            };

            Assert.AreEqual(Opcode.Set, request.RequestOpcode);

            var queryBuffer = request.GetQueryBuffer();
            CollectionAssert.AreEqual(SET_QUERY, queryBuffer, "The set query buffer is different from the expected one");

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
                CallBackPolicy = CallBackPolicy.AllOK
            };

            var queryBuffer = request.GetQueryBuffer();
            CollectionAssert.AreEqual(SET_QUERY, queryBuffer, "The set query buffer is different from the expected one");

            Assert.DoesNotThrow(() => request.Fail());
            Assert.AreEqual(Status.InternalError, status, "The status sent by a fail should be InternalError");
        }

        /// <summary>
        /// Test Set request with redundancy = 2
        /// </summary>
        [Test]
        public void RedundantSetRequestTest()
        {
            Status status = Status.UnknownCommand;
            var request = new SetRequest
            {
                Key = @"Hello",
                Message = System.Text.UTF8Encoding.Default.GetBytes(@"World"),
                RequestId = 0,
                Expire = TimeSpan.FromHours(1),
                CallBack = (s) => status = s,
                CallBackPolicy = CallBackPolicy.AllOK,
                Replicas = 1,
            };

            var headerOK = new MemcacheResponseHeader { Opcode = Opcode.Set, Status = Status.NoError }; 
            
            Assert.AreEqual(Opcode.Set, request.RequestOpcode);

            var queryBuffer = request.GetQueryBuffer();
            CollectionAssert.AreEqual(SET_QUERY, queryBuffer, "The set query buffer is different from the expected one");

            Assert.DoesNotThrow(() => request.HandleResponse(headerOK, null, SET_EXTRA, SET_MESSAGE));
            Assert.AreEqual(Status.UnknownCommand, status, "Callback should not be called on the first OK response");
            Assert.DoesNotThrow(() => request.HandleResponse(headerOK, null, SET_EXTRA, SET_MESSAGE)); 
            Assert.AreEqual(Status.NoError, status, "Callback should be called with status OK");
        }

        /// <summary>
        /// Test Set request with redundancy = 2
        /// </summary>
        [Test]
        public void RedundantSetRequestFailTest()
        {
            Status status = Status.UnknownCommand;
            var request = new SetRequest
            {
                Key = @"Hello",
                Message = System.Text.UTF8Encoding.Default.GetBytes(@"World"),
                RequestId = 0,
                Expire = TimeSpan.FromHours(1),
                CallBack = (s) => status = s,
                CallBackPolicy = CallBackPolicy.AllOK,
                Replicas = 1,
            };

            var headerOK = new MemcacheResponseHeader { Opcode = Opcode.Set, Status = Status.NoError };
            var headerFail = new MemcacheResponseHeader { Opcode = Opcode.Set, Status = Status.OutOfMemory };

            var queryBuffer = request.GetQueryBuffer();
            CollectionAssert.AreEqual(SET_QUERY, queryBuffer, "The set query buffer is different from the expected one");

            // 1. First reponse is OK, second is failing

            Assert.DoesNotThrow(() => request.HandleResponse(headerOK, null, SET_EXTRA, SET_MESSAGE));
            Assert.AreEqual(Status.UnknownCommand, status, "Callback should not be called on the first OK response");
            Assert.DoesNotThrow(() => request.Fail());
            Assert.AreEqual(Status.InternalError, status, "The status sent by a fail should be InternalError");

            // 2. First response is failing, second one is OK

            status = Status.UnknownCommand;
            request = new SetRequest
            {
                Key = @"Hello",
                Message = System.Text.UTF8Encoding.Default.GetBytes(@"World"),
                RequestId = 0,
                Expire = TimeSpan.FromHours(1),
                CallBack = (s) => status = s,
                CallBackPolicy = CallBackPolicy.AllOK,
                Replicas = 1,
            };
      
            Assert.DoesNotThrow(() => request.HandleResponse(headerFail, null, SET_EXTRA, SET_MESSAGE));
            Assert.AreEqual(Status.OutOfMemory, status, "Callback should be called on the first failed response");
            status = Status.UnknownCommand;
            Assert.DoesNotThrow(() => request.HandleResponse(headerOK, null, SET_EXTRA, SET_MESSAGE));
            Assert.AreEqual(Status.UnknownCommand, status, "Callback should not be called on the responses following a fail");
        }

    }
}
