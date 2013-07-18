using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;

using NUnit.Framework;

using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;
using Criteo.Memcache.Locator;

using Criteo.Memcache.UTest.Mocks;

namespace Criteo.Memcache.UTest
{
    [TestFixture]
    public class LocatorTests
    {
        private IList<IMemcacheNode> _nodes;

        [TestFixtureSetUp]
        public void Setup()
        {
            _nodes = new List<IMemcacheNode>();
            _nodes.Add(new NodeMock { EndPoint = new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 1 }), 11211) });
            _nodes.Add(new NodeMock { EndPoint = new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 2 }), 11211) });
            _nodes.Add(new NodeMock { EndPoint = new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 3 }), 11211) });
            _nodes.Add(new NodeMock { EndPoint = new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 4 }), 11211) });
            _nodes.Add(new NodeMock { EndPoint = new IPEndPoint(new IPAddress(new byte[] { 192, 168, 18, 5 }), 11211) });
        }

        [Test]
        public void RoundRobinTest()
        {
            var locator = new RoundRobinLocator();
            locator.Initialize(_nodes);

            var nodeSet = new HashSet<IMemcacheNode>();

            for (int i = 0; i < _nodes.Count; ++i)
            {
                var choosenNode = locator.Locate(i.ToString());
                Assert.IsNotNull(choosenNode, "RoundRobinLocator found no node");
                nodeSet.Add(choosenNode);
            }

            Assert.AreEqual(_nodes.Count, nodeSet.Count, "All nodes should have been choosen at least once");
        }

        [Test]
        public void RoundRobinDeadNodeDetectionTest()
        {
            var locator = new RoundRobinLocator();
            locator.Initialize(_nodes);

            for (int i = 1; i < _nodes.Count; ++i)
                (_nodes[i] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                var choosenNode = locator.Locate(i.ToString());
                Assert.IsNotNull(choosenNode, "RoundRobinLocator found no node, but at least 1 in alive");
                Assert.IsFalse(choosenNode.IsDead, "RoundRobinLocator returned a dead node");
            }

            (_nodes[0] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                var choosenNode = locator.Locate(i.ToString());
                Assert.IsNull(choosenNode, "RoundRobinLocator found no node when all are dead");
            }

            for (int i = 0; i < _nodes.Count; ++i)
                (_nodes[i] as NodeMock).IsDead = false;
        }

        [Test]
        public void KetamaDeadNodeDetectionTest()
        {
            var locator = new KetamaLocator();
            locator.Initialize(_nodes);

            for (int i = 1; i < _nodes.Count; ++i)
                (_nodes[i] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                var choosenNode = locator.Locate(i.ToString());
                Assert.IsNotNull(choosenNode, "RoundRobinLocator found no node, but at least 1 in alive");
                Assert.IsFalse(choosenNode.IsDead, "RoundRobinLocator returned a dead node");
            }

            (_nodes[0] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                var choosenNode = locator.Locate(i.ToString());
                Assert.IsNull(choosenNode, "RoundRobinLocator found no node when all are dead");
            }

            for (int i = 0; i < _nodes.Count; ++i)
                (_nodes[i] as NodeMock).IsDead = false;
        }
    }
}
