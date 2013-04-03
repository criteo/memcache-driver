using System;
using System.Net;
using System.Threading;

using NUnit.Framework;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Transport;

using Criteo.Memcache.UTest.Mocks;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class SynchTransportTest
    {
        private IPAddress LOCALHOST = new IPAddress(new byte[] { 127, 0, 0, 1 });

        [Test]
        public void MemcacheTransportSynchronousThreadedTest()
        {
            var endPoint = new IPEndPoint(LOCALHOST, 11213);
            MemcacheSocketSynchronousTest(() => new MemcacheSocketSynchronous(endPoint, null, null, 0, 0, _ => { }, true), endPoint);
        }

        [Test]
        public void MemcacheTransportSynchronousAsyncTest()
        {
            var endPoint = new IPEndPoint(LOCALHOST, 11214);
            MemcacheSocketSynchronousTest(() => new MemcacheSocketSynchronous(endPoint, null, null, 0, 0, _ => { }, false), endPoint);
        }

        public void MemcacheSocketSynchronousTest(Func<IMemcacheTransport> transportFactory, IPEndPoint endPoint)
        {

            using (var serverMock = new ServerMock(endPoint))
            {
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
                    KeyLength = 5,
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
                serverMock.ResponseHeader = responseHeader;

                var request = new RequestMock
                {
                    QueryBuffer = requestBuffer,
                    RequestId = requestHeader.Opaque
                    /*ResponseHeader = responseHeader,
                    Message = responseMessage,
                    Extra = responseExtra,*/
                };

                using (var transport = transportFactory())
                {
                    Assert.IsTrue(transport.TrySend(request));

                    Assert.IsTrue(request.Mutex.Wait(TimeSpan.FromSeconds(10)), "The request has not been completed on less than 10 sec");

                    // and now, assert that we sent what we had to send and we received what the server sent
                    Assert.AreEqual(requestHeader, serverMock.LastReceivedHeader, "Sent header differ from header received by the server");
                    CollectionAssert.AreEqual(requestBody, serverMock.LastReceivedBody, "Sent body is different than received by the server");

                    Assert.AreEqual(responseHeader, request.ResponseHeader, "Received header differ from header sent by the server");
                    CollectionAssert.AreEqual(responseExtra, request.Extra, "Received extra is different than sent by the server");
                    CollectionAssert.AreEqual(responseMessage, request.Message, "Received message is different than sent by the server");
                    //Thread.Sleep(1000);
                }
            }
        }
    }
}
