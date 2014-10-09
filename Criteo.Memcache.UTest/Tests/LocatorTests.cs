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

using NUnit.Framework;

using Criteo.Memcache.Locator;
using Criteo.Memcache.Node;
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
            _nodes = new List<IMemcacheNode>
            {
                new NodeMock {EndPoint = new IPEndPoint(new IPAddress(new byte[] {192, 168, 18, 1}), 11211)},
                new NodeMock {EndPoint = new IPEndPoint(new IPAddress(new byte[] {192, 168, 18, 2}), 11211)},
                new NodeMock {EndPoint = new IPEndPoint(new IPAddress(new byte[] {192, 168, 18, 3}), 11211)},
                new NodeMock {EndPoint = new IPEndPoint(new IPAddress(new byte[] {192, 168, 18, 4}), 11211)},
                new NodeMock {EndPoint = new IPEndPoint(new IPAddress(new byte[] {192, 168, 18, 5}), 11211)},
            };
        }

        [Test]
        public void RoundRobinTest()
        {
            var locator = new RoundRobinLocator();
            locator.Initialize(_nodes);

            var nodeSet = new HashSet<IMemcacheNode>();

            for (int i = 0; i < _nodes.Count; ++i)
            {
                var key = i.ToString().Select(c => (byte)c).ToArray();

                var locations = locator.Locate(key);
                Assert.IsNotEmpty(locations, "RoundRobinLocator found no node");

                var chosenNode = locations.First<IMemcacheNode>();
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

            // Test several keys, and iterate through each key several times
            for (int idx = 0; idx < _nodes.Count; ++idx)
            {
                var nodeSet = new HashSet<IMemcacheNode>();

                int nodeCount = 0;
                var keyAsBytes = idx.ToString().Select(c => (byte)c).ToArray();
                foreach (var chosenNode in locator.Locate(keyAsBytes))
                {
                    Assert.IsNotNull(chosenNode, "KetamaLocator found no node");
                    nodeSet.Add(chosenNode);
                    nodeCount++;
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
                var keyAsBytes = i.ToString().Select(c => (byte)c).ToArray();

                var locations = locator.Locate(keyAsBytes);
                Assert.IsNotEmpty(locations, "RoundRobinLocator found no node, but at least 1 is alive");

                var chosenNode = locations.First();
                Assert.IsFalse(chosenNode.IsDead, "RoundRobinLocator returned a dead node");
            }

            (_nodes[0] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                var keyAsBytes = i.ToString().Select(c => (byte)c).ToArray();
                CollectionAssert.IsEmpty(locator.Locate(keyAsBytes), "RoundRobinLocator found a node when all are dead");
            }

            foreach (var node in _nodes)
                (node as NodeMock).IsDead = false;
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
                var keyAsBytes = i.ToString().Select(c => (byte)c).ToArray();

                var locations = locator.Locate(keyAsBytes);
                Assert.IsNotEmpty(locations, "KetamaLocator found no node, but at least one is alive");

                var chosenNode = locations.First<IMemcacheNode>();
                Assert.IsFalse(chosenNode.IsDead, "KetamaLocator returned a dead node");
            }

            // Now all nodes are dead
            (_nodes[0] as NodeMock).IsDead = true;

            for (int i = 0; i < _nodes.Count; ++i)
            {
                var keyAsBytes = i.ToString().Select(c => (byte)c).ToArray();
                CollectionAssert.IsEmpty(locator.Locate(keyAsBytes), "KetamaLocator found a node when all are dead");
            }

            foreach (var node in _nodes)
                (node as NodeMock).IsDead = false;
        }

        // Same test but killing the nodes after the enumeration has started. The Locator should still filter the dead nodes.
        [Test]
        public void KetamaDeadNodeDetectionTest2()
        {
            var locator = new KetamaLocator();
            locator.Initialize(_nodes);

            for (int i = 0; i < _nodes.Count; ++i)
            {
                foreach (IMemcacheNode node in _nodes)
                    (node as NodeMock).IsDead = false;

                var keyAsBytes = i.ToString().Select(c => (byte)c).ToArray();
                var locations = locator.Locate(keyAsBytes);

                var j = 0;
                var nodesSet = new HashSet<IMemcacheNode>();
                foreach (var chosenNode in locations)
                {
                    nodesSet.Add(chosenNode);
                    Assert.IsFalse(chosenNode.IsDead, "KetamaLocator returned a dead node");
                    // Kill one of the nodes
                    (_nodes[j++] as NodeMock).IsDead = true;
                }

                Assert.AreNotEqual(0, nodesSet.Count, "KetamaLocator returned no node");
            }

            foreach (var node in _nodes)
                (node as NodeMock).IsDead = false;
        }
    }
}
