using System;
using System.Net;
using System.Threading;

using NUnit.Framework;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Transport;
using Criteo.Memcache.UTest.Mocks;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class TransportTest
    {
        private IPAddress LOCALHOST = new IPAddress(new byte[] { 127, 0, 0, 1 });

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
                var requestBuffer = new byte[MemcacheRequestHeader.SIZE + requestHeader.TotalBodyLength];
                requestHeader.ToData(requestBuffer);
                Array.Copy(requestBody, 0, requestBuffer, MemcacheRequestHeader.SIZE, requestHeader.TotalBodyLength);

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

                using (var transport = new MemcacheTransport(endPoint, new MemcacheClientConfiguration(), _ => { }, _ => { }, false))
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
                TransportQueueLength = 1,
            };
            int transportAvailablized = 0;

            using (var serverMock = new ServerMock())
            using (var transportToTest = new MemcacheTransport(serverMock.ListenEndPoint, config, t => { }, t => Interlocked.Increment(ref transportAvailablized), false))
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
                    Key = "Hello, world",
                    RequestOpcode = Opcode.Get,
                    CallBack = (r, v) => clientMutex.Set(),
                };
                var request2 = new GetRequest
                {
                    RequestId = (uint)42,
                    Key = "Hello, world",
                    RequestOpcode = Opcode.Get,
                    CallBack = (r, v) => { },
                };

                // we sent a first request and let the server wait before respond
                Assert.IsTrue(transportToTest.TrySend(request1), "The first request failed to be sent");
                Assert.That(() => transportAvailablized, Is.EqualTo(1).After(1000, 50));
                // we check that the queue is full, and the transport fail to send a new request
                Assert.IsFalse(transportToTest.TrySend(request2), "The second request should not have been sent");
                // unblocks both server response and callback from client
                serverMock.ReceiveMutex.Set();
                Assert.IsTrue(clientMutex.Wait(1000), "The response callback has not been triggered for the first request");
                // make sure that we triggered the transport available after the queue is not full anymore
                Assert.That(() => transportAvailablized, Is.EqualTo(2).After(1000, 50));
                // checks if we can send a new request since the queue is not full anymore
                Assert.IsTrue(transportToTest.TrySend(request2), "The third request failed to be sent");
                Assert.That(() => transportAvailablized, Is.EqualTo(3).After(1000, 50));
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
                t => {},
                t => {}, false))
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
    }
}
