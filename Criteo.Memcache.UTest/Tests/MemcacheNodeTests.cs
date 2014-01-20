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
using System.Collections.Generic;
using System.Net;
using System.Threading;

using NUnit.Framework;
using NUnit.Framework.Constraints;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;
using Criteo.Memcache.UTest.Mocks;

namespace Criteo.Memcache.UTest.Tests
{
    /// <summary>
    /// Test around the MemcacheNode object
    /// </summary>
    [TestFixture]
    public class MemcacheNodeTests
    {
        [Test]
        public void SyncNodeDeadDetection()
        {
            bool aliveness = true;
            var transportMocks = new List<TransportMock>();
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                TransportFactory = (_, __, r, s, ___, ____) =>
                    {
                        var transport = new TransportMock(r) { IsAlive = aliveness, Setup = s };
                        transportMocks.Add(transport);
                        return transport;
                    },
                PoolSize = 2,
            };
            var node = new MemcacheNode(null, config, null);
            CollectionAssert.IsNotEmpty(transportMocks, "No transport has been created by the node");

            Assert.IsTrue(node.TrySend(new NoOpRequest(), Timeout.Infinite), "Unable to send a request throught the node");

            // creation of new transport will set them as dead
            aliveness = false;
            foreach (var transport in transportMocks)
                transport.IsAlive = false;

            Assert.IsFalse(node.TrySend(new NoOpRequest(), Timeout.Infinite), "The node did not failed with all transport deads");

            foreach (var transport in transportMocks)
                transport.IsAlive = true;

            Assert.IsFalse(node.IsDead, "The node is still dead, should be alive now !");
            Assert.IsTrue(node.TrySend(new NoOpRequest(), Timeout.Infinite), "Unable to send a request throught the node after it's alive");
        }

        private IPAddress LOCALHOST = new IPAddress(new byte[] { 127, 0, 0, 1 });

        [Test]
        public void ReceiveFailConsistency([Values(true, false)] bool failInBody)
        {
            using (var serverMock = new ServerMock())
            {
                var endpoint = serverMock.ListenEndPoint;
                serverMock.ResponseBody = new byte[24];
                if (failInBody)
                {
                    // the Opaque vs RequestId is check in the body receiving callback
                    // I put 2 different values
                    new MemcacheResponseHeader
                    {
                        Cas = 8,
                        DataType = 12,
                        ExtraLength = 0,
                        KeyLength = 0,
                        Opaque = 0,
                        Status = Status.NoError,
                        Opcode = Opcode.Set,
                        TotalBodyLength = 0,
                    }.ToData(serverMock.ResponseHeader);
                }
                else
                {
                    // the magic number is checked in the header receiving callback
                    // I corrupt it
                    new MemcacheResponseHeader
                    {
                        Cas = 8,
                        DataType = 12,
                        ExtraLength = 0,
                        KeyLength = 0,
                        Opaque = 1,
                        Status = Status.NoError,
                        Opcode = Opcode.Set,
                        TotalBodyLength = 0,
                    }.ToData(serverMock.ResponseHeader);
                    serverMock.ResponseHeader.CopyFrom(0, (uint)42);
                }

                var config = new MemcacheClientConfiguration
                {
                    PoolSize = 1,
                };

                var node = new Memcache.Node.MemcacheNode(endpoint, config, null);
                var errorMutex = new ManualResetEventSlim(false);
                var callbackMutex = new ManualResetEventSlim(false);

                Exception expectedException = null;
                node.TransportError += e =>
                    {
                        Interlocked.Exchange<Exception>(ref expectedException, e);
                        errorMutex.Set();
                    };
                int nodeAliveCount = 0;
                node.NodeAlive += t => ++nodeAliveCount;
                int nodeDeadCount = 0;
                node.NodeDead += t => ++nodeDeadCount;

                Status receivedStatus = Status.NoError;
                node.TrySend(
                    new SetRequest
                    {
                        RequestOpcode = Opcode.Set,
                        RequestId = 1,
                        Expire = TimeSpan.FromSeconds(1),
                        Key = @"Key",
                        Message = new byte[] { 0, 1, 2, 3, 4 },
                        CallBack = s =>
                        {
                            receivedStatus = s;
                            callbackMutex.Set();
                        },
                    }, 1000);
                // must wait before the next test because the TransportError event is fired after the callback call

                // Expected result :
                // * The callback must have been called before 1 sec
                // * The failure callback must have been called, so the received status must be InternalError
                // * An MemcacheException should have been raised by the receive due to the bad response
                // * The node should have exaclty one transport in its pool
                //      more mean that we added it twice after a failure, less means we didn't putted it back in the pool
                // * The node should not be seen has dead yet

                Assert.IsTrue(callbackMutex.Wait(1000), @"The 1st callback has not been received after 1 second");
                Assert.IsTrue(errorMutex.Wait(1000), @"The 1st error has not been received after 1 second");
                Assert.AreEqual(Status.InternalError, receivedStatus, @"A bad response has not sent an InternalError to the request callback");
                Assert.IsInstanceOf<Memcache.Exceptions.MemcacheException>(expectedException, @"A bad response has not triggered a transport error. Expected a MemcacheException.");
                Assert.AreEqual(0, nodeDeadCount, @"The node has been detected has dead before a new send has been made");
                // After a short delay, the transport should be back in the transport pool (node.PoolSize == 1)
                Assert.That(() => { return node.PoolSize; }, new DelayedConstraint(new EqualConstraint(1), 2000, 100), "After a while, the transport should be back in the pool");

                new MemcacheResponseHeader
                {
                    Cas = 8,
                    DataType = 12,
                    ExtraLength = 0,
                    KeyLength = 0,
                    // must be the same or it will crash : TODO add a test to ensure we detect this fail
                    Opaque = 1,
                    Status = Status.NoError,
                    Opcode = Opcode.Set,
                    TotalBodyLength = 0,
                }.ToData(serverMock.ResponseHeader);

                serverMock.ResponseBody = null;
                expectedException = null;
                callbackMutex.Reset();
                receivedStatus = Status.NoError;

                var result = node.TrySend(
                    new SetRequest
                    {
                        RequestOpcode = Opcode.Set,
                        RequestId = 1,
                        Expire = TimeSpan.FromSeconds(1),
                        Key = @"Key",
                        Message = new byte[] { 0, 1, 2, 3, 4 },
                        CallBack = s =>
                        {
                            receivedStatus = s;
                            callbackMutex.Set();
                        },
                    }, 1000);

                // Expected result :
                // * An SocketException should have been raised by the send, since the previous receice has disconnected the socket
                // * The return must be true, because the request have been enqueued before the transport seen the socket died
                // * The failure callback must have been called, so the received status must be InternalError

                Assert.IsTrue(callbackMutex.Wait(1000), @"The 2nd callback has not been received after 1 second");
                Assert.IsTrue(result, @"The first failed request should not see a false return");
                Assert.AreEqual(Status.InternalError, receivedStatus, @"The send operation should have detected that the socket is dead");

                // After a short delay, the transport should connect
                Assert.That(() => { return node.PoolSize; }, new DelayedConstraint(new EqualConstraint(1), 2000, 100), "After a while, the transport should manage to connect");

                expectedException = null;
                callbackMutex.Reset();
                result = node.TrySend(
                    new SetRequest
                    {
                        RequestOpcode = Opcode.Set,
                        RequestId = 1,
                        Expire = TimeSpan.FromSeconds(1),
                        Key = @"Key",
                        Message = new byte[] { 0, 1, 2, 3, 4 },
                        CallBack = s =>
                        {
                            receivedStatus = s;
                            callbackMutex.Set();
                        },
                    }, 1000);

                // Expected result : everything should works fine now
                // * The return must be true, because the new transport should be available now
                // * No exception should have been raised
                // * The callback must have been called and with a NoError status

                Assert.IsTrue(result, @"The node has not been able to send a new request after a disconnection");
                Assert.IsTrue(callbackMutex.Wait(1000), @"The message has not been received after 1 second, case after reconnection");
                Assert.AreEqual(Status.NoError, receivedStatus, @"The response after a reconnection is still not NoError");
                Assert.IsNull(expectedException, "The request shouldn't have thrown an exception");
            }
        }

        // Test the coherency of the _workingTransports counter in MemcacheNode
        [TestCase(1)]
        [TestCase(2)]
        public void NodeWorkingTransportsTest(int nbOfTransportsPerNode)
        {
            int createdTransports = 0;
            int rememberPort;
            var mutex = new ManualResetEventSlim(false);
            Status returnStatus = Status.NoError;
            MemcacheNode theNode = null;

            // Memcache client config
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                TransportFactory = (_, __, ___, ____, _____, ______) =>
                {
                    return new MemcacheTransportForTest(_, __, ___, ____, _____, ______, () => { createdTransports++; }, () => { });
                },
                NodeFactory =  (_, __, ___) =>
                {
                    var node = MemcacheClientConfiguration.DefaultNodeFactory(_, __, ___);
                    theNode = node as MemcacheNode;
                    return node;
                },
                PoolSize = nbOfTransportsPerNode,
            };


            MemcacheClient memcacheClient;
            using (var serverMock1 = new ServerMock())
            {
                config.NodesEndPoints.Add(serverMock1.ListenEndPoint);
                rememberPort = serverMock1.ListenEndPoint.Port;

                serverMock1.ResponseBody = new byte[24];

                // Create a Memcache client with one node
                memcacheClient = new MemcacheClient(config);

                // Check that we hooked to the MemcacheNode
                Assert.IsNotNull(theNode, "Did not hook to the MemcacheNode while creating the client");

                // Check the number of transports that have been created
                Assert.AreEqual(nbOfTransportsPerNode, createdTransports);

                // Check the number of working transports (meaning, connected to the server) and the node state
                // By default, the transport are marked as being available upon creation of the client
                Assert.AreEqual(nbOfTransportsPerNode, theNode.WorkingTransports);
                Assert.IsFalse(theNode.IsDead);

                // Do a get to initialize one of the transports
                Assert.IsTrue(memcacheClient.Get("whatever", (s, o) => { returnStatus = s; mutex.Set(); }));
                Assert.IsTrue(mutex.Wait(1000), "Timeout on the get request");
                Assert.AreEqual(Status.InternalError, returnStatus);
                mutex.Reset();

                // Check the number of working transports and the node state
                Assert.AreEqual(nbOfTransportsPerNode, theNode.WorkingTransports);
                Assert.IsFalse(theNode.IsDead);
            }

            // Wait for the ServerMock to be fully disposed
            Thread.Sleep(100);

            // Attempt to send a request to take one of the transports out of the pool
            // After that the node should be dead
            Assert.IsTrue(memcacheClient.Get("whatever", (s, o) => { returnStatus = s; mutex.Set(); }));
            Assert.IsTrue(mutex.Wait(1000), "Timeout on the get request");
            Assert.AreEqual(Status.InternalError, returnStatus);
            mutex.Reset();

            // Check the number of working transports and the node state
            Assert.AreEqual(nbOfTransportsPerNode - 1, theNode.WorkingTransports);
            if (nbOfTransportsPerNode == 1)
                Assert.IsTrue(theNode.IsDead);
            else
                Assert.IsFalse(theNode.IsDead);

            // A new transport has been allocated and is periodically trying to reconnect
            Assert.AreEqual(nbOfTransportsPerNode + 1, createdTransports, "Expected a new transport to be created to replace the disposed one");

            using (var serverMock2 = new ServerMock(rememberPort))
            {
                serverMock2.ResponseBody = new byte[24];

                // After some delay, the transport should connect
                Assert.That(() => { return theNode.WorkingTransports; }, new DelayedConstraint(new EqualConstraint(nbOfTransportsPerNode), 4000, 100), "After a while, the transport should manage to connect");
                Assert.IsFalse(theNode.IsDead);

                // Attempt a get
                Assert.IsTrue(memcacheClient.Get("whatever", (s, o) => { returnStatus = s; mutex.Set(); }));
                Assert.IsTrue(mutex.Wait(1000), "Timeout on the get request");
                Assert.AreEqual(Status.InternalError, returnStatus);
                mutex.Reset();

                // Check the number of working transports and the node state
                Assert.AreEqual(nbOfTransportsPerNode, theNode.WorkingTransports);
                Assert.IsFalse(theNode.IsDead);

                // Dispose the client
                memcacheClient.Dispose();
            }
        }
    }
}
