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
using System;
using System.Linq;
using System.Net;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using Moq;
using NUnit.Framework;

using Criteo.Memcache.Cluster;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Locator;
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Serializer;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class ClientTests
    {
        private const string CallContextKey = "MemcacheClientTest";

        IMemcacheCluster _clusterMock;
        INodeLocator _locatorMock;
        IMemcacheNode _nodeMock;
        MemcacheResponseHeader _responseHeader;
        byte[] _key, _extra, _message;

        [SetUp]
        public void Setup()
        {
            CallContext.LogicalSetData(CallContextKey, "Fail");

            var dummyExecutionContext = ExecutionContext.Capture();

            CallContext.FreeNamedDataSlot(CallContextKey);

            var nodeMock = new Mock<IMemcacheNode>();
            nodeMock.Setup(n => n.EndPoint).Returns(new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 11211));
            nodeMock.Setup(n => n.IsDead).Returns(false);
            nodeMock.Setup(n => n.TrySend(It.IsAny<IMemcacheRequest>(), It.IsAny<int>()))
                .Callback((IMemcacheRequest req, int timeout) => ExecutionContext.Run(dummyExecutionContext, _ => req.HandleResponse(_responseHeader, _key, _extra, _message), null))
                .Returns(true);
            _nodeMock = nodeMock.Object;

            var locatorMoq = new Mock<INodeLocator>();
            locatorMoq.Setup(l => l.Locate(It.IsAny<IMemcacheRequest>())).Returns(Enumerable.Repeat(_nodeMock, 1));
            _locatorMock = locatorMoq.Object;

            var clusterMoq = new Mock<IMemcacheCluster>();
            clusterMoq.Setup(c => c.Locator).Returns(_locatorMock);
            clusterMoq.Setup(c => c.Nodes).Returns(Enumerable.Repeat(_nodeMock, 1));
            _clusterMock = clusterMoq.Object;

            _key = null;
            _extra = null;
            _message = null;
            _responseHeader = default(MemcacheResponseHeader);
        }

        public class TestType { }
        class SerializationException : Exception { }
        [Test]
        public void SerializationThrows()
        {
            var serializerMoq = new Mock<ISerializer<TestType>>();
            serializerMoq.Setup(s => s.FromBytes(It.IsAny<byte[]>())).Throws(new SerializationException());
            serializerMoq.Setup(s => s.ToBytes(It.IsAny<TestType>())).Throws(new SerializationException());
            serializerMoq.Setup(s => s.TypeFlag).Returns(314);

            Exception raised = null;

            var config = new MemcacheClientConfiguration
            {
                ClusterFactory = c => _clusterMock,
            };
            var serialiserMoqObj = serializerMoq.Object;
            config.SetSerializer(serialiserMoqObj);
            var client = new MemcacheClient(config);
            client.CallbackError += e => raised = e;

            // test that the throws of the serializer is synchronously propagated
            Assert.Throws(typeof(SerializationException), () => client.Set("Hello", new TestType(), TimeSpan.Zero));

            // test that the failing serializer does not synchronously throws but sends a CallbackError event
            _responseHeader = new MemcacheResponseHeader
            {
                ExtraLength = 4,
                TotalBodyLength = 4,
                Opcode = Opcode.Get,
            };
            _extra = new byte[] { 0, 0, 0, 0};
            Assert.True(client.Get("Hello", (Status s, TestType v) => { }));
            Assert.IsNotNull(raised);
            Assert.IsInstanceOf<SerializationException>(raised);
        }

        [Test]
        public void UnsupportedSerializationThrows()
        {
            var config = new MemcacheClientConfiguration
            {
                ClusterFactory = c => _clusterMock,
            };
            var client = new MemcacheClient(config);

            // assert that both set and get throws NotSupportedException for an unknown type
            Assert.Throws(typeof(NotSupportedException), () => client.Set("Hello", new TestType(), TimeSpan.Zero));
            Assert.Throws(typeof(NotSupportedException), () => client.Get("Hello", (Status s, TestType v) => { }));
        }

        [Test]
        public void ExecutionContextShouldFlow()
        {
            var config = new MemcacheClientConfiguration
            {
                ClusterFactory = c => _clusterMock,
            };
            var client = new MemcacheClient(config);

            object value = null;

            _extra = new byte[] { 0, 0, 0, 0 };

            CallContext.LogicalSetData(CallContextKey, "OK");

            client.Get("test", (Status s, byte[] v) =>
            {
                value = CallContext.LogicalGetData("MemcacheClientTest");
            });

            Assert.AreEqual("OK", value, "The execution context didn't flow");
        }
    }
}
