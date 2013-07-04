using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;

using NUnit.Framework;

using Criteo.Memcache.UTest.Mocks;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    class ReplicasTests
    {
        private static MemcacheClientConfiguration _configuration;

        [TestFixtureSetUp]
        public void Setup()
        {
            _configuration = new MemcacheClientConfiguration
            {
                QueueTimeout = 0,
                NodeFactory = (ipendpoint, config) => new NodeMock { EndPoint = ipendpoint, DefaultResponse = Status.NoError, },
            };

            _configuration.NodesEndPoints.Add(new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 1 }), 11211));
            _configuration.NodesEndPoints.Add(new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 2 }), 11211));
            _configuration.NodesEndPoints.Add(new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 3 }), 11211));
            _configuration.NodesEndPoints.Add(new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 4 }), 11211));
            _configuration.NodesEndPoints.Add(new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 5 }), 11211));
            _configuration.NodesEndPoints.Add(new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 6 }), 11211));
            _configuration.NodesEndPoints.Add(new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 7 }), 11211));
            _configuration.NodesEndPoints.Add(new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 8 }), 11211));
        }

        [Test]
        public void MemcacheClientReplicasTest()
        {

            var client = new MemcacheClient(_configuration);

            for (int replicas = 0; replicas < _configuration.NodesEndPoints.Count; replicas++)
            {
                _configuration.Replicas = replicas;

                // SET
                NodeMock.trySendCounter = 0;
                Assert.IsTrue(client.Set("toto", new byte[0], TimeSpan.MaxValue, null));
                Assert.AreEqual(replicas + 1, NodeMock.trySendCounter);

                // GET
                NodeMock.trySendCounter = 0;
                Assert.IsTrue(client.Get("toto", null));
                Assert.AreEqual(replicas + 1, NodeMock.trySendCounter);

                // DELETE
                NodeMock.trySendCounter = 0;
                Assert.IsTrue(client.Delete("toto", null));
                Assert.AreEqual(replicas + 1, NodeMock.trySendCounter);

            }

            // Set number of replicas to a number strictly greater than the number of nodes, minus one.
            NodeMock.trySendCounter = 0;
            _configuration.Replicas = _configuration.NodesEndPoints.Count;
            Assert.IsTrue(client.Set("toto", new byte[0], TimeSpan.MaxValue, null));
            Assert.AreEqual(_configuration.NodesEndPoints.Count, NodeMock.trySendCounter);
        }

    }
}
