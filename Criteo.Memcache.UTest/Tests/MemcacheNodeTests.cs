using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using NUnit.Framework;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Node;
using Criteo.Memcache.UTest.Mocks;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.UTest.Tests
{
    /// <summary>
    /// Test around the MemcacheNode object
    /// </summary>
    [TestFixture]
    public class MemcacheNodeTests
    {
        [Test]
        public void AsyncNodeDeadDetection()
        {
            DeadSocketMock socketMock = null;
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                SocketFactory = (_, __, n, ___, ____, _____) => (socketMock = new DeadSocketMock { WaitingRequests = n as IMemcacheRequestsQueue }),
                QueueLength = 1,
            };
            var node = new MemcacheNode(null, config, _ => { });

            Assert.IsNotNull(socketMock, "No socket has been created by the node");

            Assert.IsTrue(node.TrySend(new NoOpRequest(), Timeout.Infinite), "Unable to send a request throught the node");
            Thread.Sleep(2000);

            Assert.AreEqual(true, node.IsDead, "The node is still alive, should be dead now !");
            Thread.Sleep(2000);

            socketMock.RespondToRequest();

            Assert.AreEqual(false, node.IsDead, "The node is still dead, should be alive now !");
        }
        
        [Test]
        public void SyncNodeDeadDetection()
        {
            var transportMocks = new List<SynchTransportMock>();
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                SynchornousTransportFactory = (_, __, ___, ____, _____, s) => 
                    {
                        var transport = new SynchTransportMock { IsDead = false, Setup = s };
                        transportMocks.Add(transport);
                        return transport;
                    },
                PoolSize = 2,
            };
            var node = new MemcacheSynchNode(null, config, _ => { });
            CollectionAssert.IsNotEmpty(transportMocks, "No transport has been created by the node");
            
            Assert.IsTrue(node.TrySend(new NoOpRequest(), Timeout.Infinite), "Unable to send a request throught the node");

            foreach (var transport in transportMocks)
                transport.IsDead = true;

            Assert.IsFalse(node.TrySend(new NoOpRequest(), Timeout.Infinite), "The node did not failed with all transport deads");

            foreach (var transport in transportMocks)
                transport.IsDead = false;

            Assert.AreEqual(false, node.IsDead, "The node is still dead, should be alive now !");
            Assert.IsTrue(node.TrySend(new NoOpRequest(), Timeout.Infinite), "Unable to send a request throught the node after it's alive");
        }
    }
}
