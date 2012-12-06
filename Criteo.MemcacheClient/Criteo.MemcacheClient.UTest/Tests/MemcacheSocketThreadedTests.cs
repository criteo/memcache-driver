using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using NUnit.Framework;

using Criteo.MemcacheClient.Requests;
using Criteo.MemcacheClient.Sockets;
using Criteo.MemcacheClient.Node;
using Criteo.MemcacheClient.Headers;

using Criteo.MemcacheClient.UTest.Mocks;

namespace Criteo.MemcacheClient.UTest.Tests
{
    /// <summary>
    /// Test around the MemcacheSocketThreaded object
    /// Not working yet !
    /// </summary>
    [TestFixture]
    public class MemcacheSocketThreadedTests
    {
        [Test]
        public void MemcacheSocketThreadedTest()
        {
            var localhost = new IPEndPoint(new IPAddress(new byte[] {127, 0, 0, 1}), 11211);

            var serverMock = new ServerMock(localhost);
            var queue = new NodeQueueMock();
            var socket = new MemcacheSocketThreaded(localhost, queue);

            // random header
            var requestHeader = new MemcacheRequestHeader 
            {
                Cas = 1,
                DataType = 2,
                ExtraLength = 5,
                KeyLength = 3,
                Opaque = 42,
                Opcode = Opcode.IncrementQ,
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
                Opaque = 56,
                Status = Status.UnknownCommand,
                Opcode = Opcode.PrependQ,
                TotalBodyLength = 15,
            };
            // body with the size defined in the header
            var responseExtra = new byte[] { 1, 2, 3};
            var responseMessage = new byte[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

            // build the request buffer
            var responseBuffer = new byte[MemcacheResponseHeader.SIZE + responseHeader.TotalBodyLength];
            requestHeader.ToData(responseBuffer);
            Array.Copy(responseExtra, 0, responseBuffer, MemcacheResponseHeader.SIZE, responseHeader.ExtraLength);
            requestHeader.ToData(responseBuffer);
            Array.Copy(responseMessage, 0, responseBuffer, MemcacheResponseHeader.SIZE + responseHeader.ExtraLength, responseHeader.TotalBodyLength - responseHeader.ExtraLength);

            // set the things to answer to the server
            serverMock.ResponseBody = responseBuffer;
            serverMock.ResponseHeader = responseHeader;

            var request = new RequestMock
            {
                QueryBuffer = requestBuffer,
                ResponseHeader = responseHeader,
                Message = responseMessage,
                Extra = responseExtra,
            };
            queue.Add(request);

            Thread.Sleep(1000);

            // and now, assert that we sent what we had to send and we received what the server sent
            Assert.AreEqual(requestHeader, serverMock.LastReceivedHeader, "Sent header differ from header received by the server");
            CollectionAssert.AreEqual(requestBody, serverMock.LastReceivedBody, "Sent body is different than received by the server");

            Assert.AreEqual(responseHeader, request.ResponseHeader, "Received header differ from header sent by the server");
            CollectionAssert.AreEqual(responseExtra, request.Extra, "Received extra is different than sent by the server");
            CollectionAssert.AreEqual(responseMessage, request.Message, "Received message is different than sent by the server");
        }
    }
}
