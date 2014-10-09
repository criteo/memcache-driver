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
using System.Net;
using System.Threading;

using NUnit.Framework;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
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
            var callbackMutex = new ManualResetEventSlim(false);

            var client = new MemcacheClient(_configuration);

            // The number of requests sent is capped by the number of nodes in the cluster.
            // The last iteration of the loop below actually tests the case where the total
            // number of copies (Replicas+1) is strictly greater than the number of nodes.
            int nodeCount = _configuration.NodesEndPoints.Count;
            for (int replicas = 0; replicas <= nodeCount; replicas++)
            {
                _configuration.Replicas = replicas;

                // SET
                callbackMutex.Reset();
                NodeMock.TrySendCounter = 0;
                Assert.IsTrue(client.Set("toto", new byte[0], TimeSpan.MaxValue, s => callbackMutex.Set()));
                Assert.AreEqual(Math.Min(replicas + 1, nodeCount), NodeMock.TrySendCounter);
                Assert.IsTrue(callbackMutex.Wait(1000),
                    string.Format("The SET callback has not been received after 1 second (Replicas = {0})", replicas));

                // GET
                callbackMutex.Reset();
                NodeMock.TrySendCounter = 0;
                Assert.IsTrue(client.Get("toto", (s, data) => callbackMutex.Set()));
                Assert.AreEqual(Math.Min(replicas + 1, nodeCount), NodeMock.TrySendCounter);
                Assert.IsTrue(callbackMutex.Wait(1000),
                    string.Format("The GET callback has not been received after 1 second (Replicas = {0})", replicas));

                // DELETE
                callbackMutex.Reset();
                NodeMock.TrySendCounter = 0;
                Assert.IsTrue(client.Delete("toto", s => callbackMutex.Set()));
                Assert.AreEqual(Math.Min(replicas + 1, nodeCount), NodeMock.TrySendCounter);
                Assert.IsTrue(callbackMutex.Wait(1000),
                    string.Format("The DELETE callback has not been received after 1 second (Replicas = {0})", replicas));

            }
        }

    }
}
