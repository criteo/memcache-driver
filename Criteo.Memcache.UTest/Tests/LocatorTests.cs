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

namespace Criteo.Memcache.UTest.Tests
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
                Assert.IsNotEmpty(locator.Locate(i.ToString()), "RoundRobinLocator found no node");
                var chosenNode = locator.Locate(i.ToString()).First<IMemcacheNode>();
                Assert.IsNotNull(chosenNode, "RoundRobinLocator found no node");
                nodeSet.Add(chosenNode);
            }

            Assert.AreEqual(_nodes.Count, nodeSet.Count, "All nodes should have been chosen at least once");
        }

        [Test]
        public void KetamaTest()
        {
            var locator = new KetamaLocator();
            locator.Initialize(_nodes);

            // Test several keys, and iterate through each key several times (to test redundancy)
            for (int idx = 0; idx < _nodes.Count; ++idx)
            {
                var nodeSet = new HashSet<IMemcacheNode>();

                int nodeCount = 0;
                foreach (var chosenNode in locator.Locate(idx.ToString()))
                {
                    Assert.IsNotNull(chosenNode, "KetamaLocator found no node");
                    nodeSet.Add(chosenNode);
                    nodeCount++;
                    if (nodeCount == _nodes.Count)
                    {
                        break;
                    }
                    Assert.AreEqual(nodeCount, nodeSet.Count, "For a given key, all nodes returned by the locator should be distinct");
                }
                Assert.AreEqual(_nodes.Count, nodeCount, "KetamaLocator did not return the maximum number of nodes available");
            }
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
                Assert.IsNotEmpty(locator.Locate(i.ToString()), "RoundRobinLocator found no node, but at least 1 is alive");
                var chosenNode = locator.Locate(i.ToString()).First<IMemcacheNode>();
                Assert.IsFalse(chosenNode.IsDead, "RoundRobinLocator returned a dead node");
            }

            (_nodes[0] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                CollectionAssert.IsEmpty(locator.Locate(i.ToString()), "RoundRobinLocator found a node when all are dead");
            }

            for (int i = 0; i < _nodes.Count; ++i)
                (_nodes[i] as NodeMock).IsDead = false;
        }

        [Test]
        public void KetamaDeadNodeDetectionTest1()
        {
            var locator = new KetamaLocator();
            locator.Initialize(_nodes);

            // All nodes but one are dead
            for (int i = 1; i < _nodes.Count; ++i)
                (_nodes[i] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                Assert.IsNotEmpty(locator.Locate(i.ToString()), "KetamaLocator found no node, but at least one is alive");
                var chosenNode = locator.Locate(i.ToString()).First<IMemcacheNode>();
                Assert.IsFalse(chosenNode.IsDead, "KetamaLocator returned a dead node");
            }

            // Now all nodes are dead
            (_nodes[0] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                CollectionAssert.IsEmpty(locator.Locate(i.ToString()), "KetamaLocator found a node when all are dead");
            }

            for (int i = 0; i < _nodes.Count; ++i)
                (_nodes[i] as NodeMock).IsDead = false;
        }

        // Same test but killing the nodes after the enumeration has started. The Locator should still filter the dead nodes.
        [Test]
        public void KetamaDeadNodeDetectionTest2()
        {
            var locator = new KetamaLocator();
            locator.Initialize(_nodes);
            
            for (int i = 0; i < _nodes.Count; ++i)
            {
                int j;
                for (j = 0; j < _nodes.Count; ++j)
                    (_nodes[j] as NodeMock).IsDead = false;

                j =  0;
                HashSet<IMemcacheNode> nodesSet = new HashSet<IMemcacheNode>();
                foreach (var chosenNode in locator.Locate(i.ToString()))
                {
                    nodesSet.Add(chosenNode);
                    Assert.IsFalse(chosenNode.IsDead, "KetamaLocator returned a dead node"); 
                    // Kill one of the nodes
                    (_nodes[j++] as NodeMock).IsDead = true;
                }
                Assert.AreNotEqual(0, nodesSet.Count, "KetamaLocator returned no node");
            }

            for (int i = 0; i < _nodes.Count; ++i)
                (_nodes[i] as NodeMock).IsDead = false;
        }
    }
}
