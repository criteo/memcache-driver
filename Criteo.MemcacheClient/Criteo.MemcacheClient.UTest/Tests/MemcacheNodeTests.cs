using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using NUnit.Framework;

using Criteo.MemcacheClient.Configuration;
using Criteo.MemcacheClient.Node;
using Criteo.MemcacheClient.UTest.Mocks;
using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.UTest.Tests
{
    [TestFixture]
    public class MemcacheNodeTests
    {
        [Test]
        public void DeadDetection()
        {
            DeadSocketMock socketMock = null;
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                SocketFactory = (_, queue) => (socketMock = new DeadSocketMock { WaitingRequests = queue }),
                QueueLength = 1,
            };
            var node = new MemcacheNode(null, config);

            Assert.IsNotNull(socketMock, "No socket has been created by the node");

            node.WaitingRequests.Add(new NoOpRequest());
            Thread.Sleep(2000);

            Assert.AreEqual(true, node.IsDead, "The node is still alive, should be dead now !");
            Thread.Sleep(2000);

            socketMock.RespondToRequest();

            Assert.AreEqual(false, node.IsDead, "The node is still dead, should be alive now !");
        }
    }
}
