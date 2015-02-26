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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

using NUnit.Framework;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Transport;
using Criteo.Memcache.UTest.Mocks;
using Criteo.Memcache.Authenticators;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class TransportTest
    {
        [Test]
        public void MemcacheSocketTest([Values(0, 2, 10)]int maxByteSentByServer)
        {
            using (var serverMock = new ServerMock())
            {
                var endPoint = serverMock.ListenEndPoint;
                serverMock.MaxSent = maxByteSentByServer;

                // random header
                var requestHeader = new MemcacheRequestHeader
                {
                    Cas = 1,
                    DataType = 2,
                    ExtraLength = 5,
                    KeyLength = 3,
                    Opaque = 42,
                    Opcode = Opcode.Increment,
                    TotalBodyLength = 12,
                };
                // body with the size defined in the header
                var requestBody = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

                // build the request buffer
                var requestBuffer = new byte[MemcacheRequestHeader.Size + requestHeader.TotalBodyLength];
                requestHeader.ToData(requestBuffer);
                Array.Copy(requestBody, 0, requestBuffer, MemcacheRequestHeader.Size, requestHeader.TotalBodyLength);

                // build the response header
                var responseHeader = new MemcacheResponseHeader
                {
                    Cas = 8,
                    DataType = 12,
                    ExtraLength = 3,
                    KeyLength = 0,
                    // must be the same or it will crash : TODO add a test to ensure we detect this fail
                    Opaque = requestHeader.Opaque,
                    Status = Status.UnknownCommand,
                    Opcode = Opcode.Prepend,
                    TotalBodyLength = 15,
                };
                // body with the size defined in the header
                var responseExtra = new byte[] { 1, 2, 3 };
                var responseMessage = new byte[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

                // build the request buffer
                var responseBody = new byte[responseHeader.TotalBodyLength];
                Array.Copy(responseExtra, 0, responseBody, 0, responseHeader.ExtraLength);
                Array.Copy(responseMessage, 0, responseBody, responseHeader.ExtraLength, responseHeader.TotalBodyLength - responseHeader.ExtraLength);

                // set the things to answer to the server
                serverMock.ResponseBody = responseBody;
                responseHeader.ToData(serverMock.ResponseHeader);

                var request = new RequestMock
                {
                    QueryBuffer = requestBuffer,
                    RequestId = requestHeader.Opaque
                    /*ResponseHeader = responseHeader,
                    Message = responseMessage,
                    Extra = responseExtra,*/
                };

                using (var transport = new MemcacheTransport(endPoint, new MemcacheClientConfiguration(), _ => { }, _ => { }, false, null))
                {
                    Assert.IsTrue(transport.TrySend(request));

                    Assert.IsTrue(request.Mutex.Wait(TimeSpan.FromSeconds(10)), "The request has not been completed on less than 10 sec");

                    // and now, assert that we sent what we had to send and we received what the server sent
                    Assert.AreEqual(requestHeader, serverMock.LastReceivedHeader, "Sent header differ from header received by the server");
                    CollectionAssert.AreEqual(requestBody, serverMock.LastReceivedBody, "Sent body is different than received by the server");

                    Assert.AreEqual(responseHeader, request.ResponseHeader, "Received header differ from header sent by the server");
                    CollectionAssert.AreEqual(responseExtra, request.Extra, "Received extra is different than sent by the server");
                    CollectionAssert.AreEqual(responseMessage, request.Message, "Received message is different than sent by the server");
                }
            }
        }

        [Test]
        public void QueueFullTest()
        {
            var config = new Configuration.MemcacheClientConfiguration
            {
                QueueLength = 1,
            };
            int transportAvailablized = 0;

            using (var serverMock = new ServerMock())
            using (var transportToTest = new MemcacheTransport(serverMock.ListenEndPoint, config, t => { }, t => Interlocked.Increment(ref transportAvailablized), false, null))
            {
                var requestHeader = new MemcacheResponseHeader
                {
                    Cas = 1,
                    DataType = 2,
                    ExtraLength = 4,
                    KeyLength = 0,
                    Opaque = 42,
                    Opcode = Opcode.Get,
                    Status = Headers.Status.KeyNotFound,
                    TotalBodyLength = 4,
                };
                requestHeader.ToData(serverMock.ResponseHeader);
                serverMock.ResponseBody = new byte[4];

                serverMock.ReceiveMutex = new ManualResetEventSlim();
                var clientMutex = new ManualResetEventSlim();
                var request1 = new GetRequest
                {
                    RequestId = (uint)42,
                    Key = "Hello, world".Select(c => (byte)c).ToArray(),
                    RequestOpcode = Opcode.Get,
                    CallBack = (r, v) => clientMutex.Set(),
                };
                var request2 = new GetRequest
                {
                    RequestId = (uint)42,
                    Key = "Hello, world".Select(c => (byte)c).ToArray(),
                    RequestOpcode = Opcode.Get,
                    CallBack = (r, v) => { },
                };

                // we sent a first request and let the server wait before respond
                Assert.IsTrue(transportToTest.TrySend(request1), "The first request failed to be sent");
                Assert.That(() => transportAvailablized, Is.EqualTo(2).After(1000, 50));
                // we check that the queue is full, and the transport fail to send a new request
                Assert.IsFalse(transportToTest.TrySend(request2), "The second request should not have been sent");
                // unblocks both server response and callback from client
                serverMock.ReceiveMutex.Set();
                Assert.IsTrue(clientMutex.Wait(1000), "The response callback has not been triggered for the first request");
                // make sure that we triggered the transport available after the queue is not full anymore
                Assert.That(() => transportAvailablized, Is.EqualTo(3).After(1000, 50));
                // checks if we can send a new request since the queue is not full anymore
                Assert.IsTrue(transportToTest.TrySend(request2), "The third request failed to be sent");
                Assert.That(() => transportAvailablized, Is.EqualTo(4).After(1000, 50));
            }
        }

        [Test]
        public void AuthenticationTest()
        {
            var config = new Configuration.MemcacheClientConfiguration
            {
                Authenticator = Configuration.MemcacheClientConfiguration.SaslPlainAuthenticatorFactory(string.Empty, "NoLogin", "NoPass"),
            };

            using (var serverMock = new ServerMock())
            using (var transport = new MemcacheTransport(serverMock.ListenEndPoint, config,
                t => { },
                t => { }, false, null))
            {
                var mutex = new ManualResetEventSlim();
                new MemcacheResponseHeader
                {
                    Opaque = 0,
                    Opcode = Opcode.StartAuth,
                    Status = Status.NoError,
                }.ToData(serverMock.ResponseHeader);

                Status responseStatus = Status.InternalError;
                Assert.IsTrue(transport.TrySend(new NoOpRequest { RequestId = 0, Callback = h => { mutex.Set(); responseStatus = h.Status; } })
                    , "Unable to send a request on authenticated transport");
                Assert.IsTrue(mutex.Wait(1000), "No response retrived on authenticated transport");
                Assert.AreEqual(Status.NoError, responseStatus, "The response returned on error");
            }
        }

        // Test that when a client is disposed, the associated Transports are also disposed
        [TestCase(1, 1)]
        [TestCase(4, 3)]
        public void MemcacheTransportDisposeBasicTest(int nbOfNodes, int nbOfTransportsPerNode)
        {
            int createdTransports = 0;
            int disposedTransports = 0;

            // Memcache client config
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                TransportConnectTimerPeriod = TimeSpan.FromMilliseconds(100),
                TransportFactory = (_, __, ___, ____, _____, ______) =>
                    new MemcacheTransportForTest(_, __, ___, ____, _____, ______, () => { createdTransports++; }, () => { disposedTransports++; }),
                PoolSize = nbOfTransportsPerNode,
            };

            var serverMocks = new List<ServerMock>(nbOfNodes);
            try
            {
                for (int p = 0; p < nbOfNodes; p++)
                {
                    var serverMock = new ServerMock();
                    config.NodesEndPoints.Add(serverMock.ListenEndPoint);
                    serverMocks.Add(serverMock);
                }

                // Create Memcache client
                var memcacheClient = new MemcacheClient(config);

                // Test the number of transports that have been created
                Assert.AreEqual(nbOfNodes * nbOfTransportsPerNode, createdTransports, "Expected number of transports = number of nodes * poolSize");

                // Dispose the client and test that the number of transports is back to zero
                memcacheClient.Dispose();
                Assert.AreEqual(createdTransports, disposedTransports, "Expected all the transports to be disposed");
            }
            finally
            {
                foreach (var mock in serverMocks)
                {
                    mock.Dispose();
                }
            }
        }

        // Test that when a client is disposed, the associated Transports are also disposed, but in a trickier case:
        // The client is disposed when one of the transports is not in the pool: for example, when the transport is
        // is disconnected from the server and trying to reconnect.
        [Test]
        public void MemcacheTransportDisposeTransportNotInPoolTest()
        {
            int createdTransports = 0;
            int disposedTransports = 0;
            var mutex1 = new ManualResetEventSlim(false);
            var mutex2 = new ManualResetEventSlim(false);
            Status returnStatus = Status.NoError;

            // Memcache client config
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                TransportConnectTimerPeriod = TimeSpan.FromMilliseconds(100),
                TransportFactory = (_, __, ___, ____, _____, ______) =>
                    new MemcacheTransportForTest(_, __, ___, ____, _____, ______, () => { createdTransports++; }, () => { disposedTransports++; mutex1.Set(); }),
                PoolSize = 1,
            };

            MemcacheClient memcacheClient;
            using (var serverMock = new ServerMock())
            {
                config.NodesEndPoints.Add(serverMock.ListenEndPoint);
                serverMock.ResponseBody = new byte[24];

                // Create Memcache client
                memcacheClient = new MemcacheClient(config);

                // Test the number of transports that have been created
                Assert.AreEqual(1, createdTransports);

                // Do a get to initialize the transport
                Assert.IsTrue(memcacheClient.Get("whatever", (s, o) => { returnStatus = s; mutex2.Set(); }));
                Assert.IsTrue(mutex2.Wait(1000), "Timeout on the get request");
                Assert.AreEqual(Status.InternalError, returnStatus);
                mutex2.Reset();
            }

            // Wait for the ServerMock to be fully disposed
            Thread.Sleep(100);

            // Attempt to send a request to take the transport out of the pool
            Assert.IsTrue(memcacheClient.Get("whatever", (s, o) => { returnStatus = s; mutex2.Set(); }));
            Assert.IsTrue(mutex2.Wait(1000), "Timeout on the get request");
            Assert.AreEqual(Status.InternalError, returnStatus);
            mutex2.Reset();

            // The initial transport should now be disposed, a new transport has been allocated and
            // is periodically trying to reconnect
            Assert.IsTrue(mutex1.Wait(1000), "Timeout on initial transport disposal");
            Assert.AreEqual(1, disposedTransports, "Expected the initial transport to be disposed");
            Assert.AreEqual(2, createdTransports, "Expected a new transport to be created to replace the disposed one");

            mutex1.Reset();

            // Dispose the client
            memcacheClient.Dispose();

            // Wait enough time for the reconnect timer to fire at least once
            Assert.IsTrue(mutex1.Wait(4000), "MemcacheTransport was not disposed before the timeout");

            // Check that all transports have been disposed
            Assert.AreEqual(2, disposedTransports);
            Assert.AreEqual(createdTransports, disposedTransports);
        }

        [Test]
        public void AuthenticationFailed()
        {
            var sentMutex = new ManualResetEventSlim(false);

            using (var serverStub = new ServerMock())
            {
                IMemcacheRequest authenticationRequest = null;
                // a token that fails
                var authenticatorTokenFailing = new Moq.Mock<IAuthenticationToken>();
                authenticatorTokenFailing
                    .Setup(t => t.StepAuthenticate(Moq.It.IsAny<TimeSpan>(), out authenticationRequest))
                    .Returns(Status.TemporaryFailure);
                // a token that works
                var authenticatorTokenOk = new Moq.Mock<IAuthenticationToken>();
                authenticatorTokenOk
                    .Setup(t => t.StepAuthenticate(Moq.It.IsAny<TimeSpan>(), out authenticationRequest))
                    .Returns(Status.NoError);
                // an authenticator that returns one failing token followed by working tokens
                bool alreadyFailed = false;
                var authenticator = new Moq.Mock<IMemcacheAuthenticator>();
                authenticator
                    .Setup(auth => auth.CreateToken())
                    .Returns(() =>
                        {
                            if (alreadyFailed)
                                return authenticatorTokenOk.Object;
                            alreadyFailed = true;
                            return authenticatorTokenFailing.Object;
                        });

                // setup the request to send
                bool requestFailed = false;
                bool requestAchieved = false;
                var request = new Moq.Mock<IMemcacheRequest>();
                request
                    .Setup(r => r.Fail())
                    .Callback(() =>
                        {
                            requestFailed = true;
                            sentMutex.Set();
                        });
                request
                    .Setup(r => r.HandleResponse(
                        Moq.It.Is<Headers.MemcacheResponseHeader>(h => h.Status == Status.NoError),
                        Moq.It.IsAny<byte[]>(),
                        Moq.It.IsAny<byte[]>(),
                        Moq.It.IsAny<byte[]>()))
                    .Callback(() =>
                        {
                            requestAchieved = true;
                            sentMutex.Set();
                        });
                var queryBuffer = new byte[MemcacheRequestHeader.Size];
                new MemcacheRequestHeader().ToData(queryBuffer);
                request
                    .Setup(r => r.GetQueryBuffer())
                    .Returns(queryBuffer);

                IMemcacheTransport transportToWork = null;
                var transportToFail = new MemcacheTransport(
                    serverStub.ListenEndPoint,
                    new MemcacheClientConfiguration
                    {
                        SocketTimeout = TimeSpan.Zero,
                        Authenticator = authenticator.Object,
                    },
                    _ => { },
                    t =>
                    {
                        Interlocked.Exchange(ref transportToWork, t);
                    },
                    false,
                    () => false);
                new MemcacheResponseHeader
                    {
                        Status = Status.NoError,
                        Opcode = Opcode.Get,
                    }.ToData(serverStub.ResponseHeader);

                Exception raised = null;
                transportToFail.TransportError += e =>
                        // when the transport fails collect the exception
                        Interlocked.Exchange(ref raised, e);
                var sent = transportToFail.TrySend(request.Object);

                Assert.IsFalse(sent, "The request send should fail");
                Assert.IsNotNull(raised, "The authentication should have failed");

                // wait for reconnection to happen (should be done in a instant timer)
                Assert.That(ref transportToWork, (!Is.Null).After(1000, 10), "The working transport should have been set");

                sent = transportToWork.TrySend(request.Object);
                Assert.IsTrue(sent, "The request should have been sent");
                var received = sentMutex.Wait(TimeSpan.FromMinutes(5));
                Assert.IsTrue(received, "The response should have been received");
                Assert.IsFalse(requestFailed, "The request should not have failed");
                Assert.IsTrue(requestAchieved, "The request should have achieved");
            }
        }
    }
}
