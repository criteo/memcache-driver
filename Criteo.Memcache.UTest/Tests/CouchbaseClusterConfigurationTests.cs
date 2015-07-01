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
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

using Criteo.Memcache.Cluster;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Locator;

using NUnit.Framework;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class CouchbaseClusterConfigurationTests
    {
        private class TestCounters
        {
            private int _errors;
            private int _nodesAdded;
            private int _nodesRemoved;

            public TestCounters()
            {
                Reset();
            }

            public void Reset()
            {
                _errors = 0;
                _nodesAdded = 0;
                _nodesRemoved = 0;
            }

            public int Errors { get { return _errors; } }

            public int NodesAdded { get { return _nodesAdded; } }

            public int NodesRemoved { get { return _nodesRemoved; } }

            public void IncrementErrors()
            {
                Interlocked.Increment(ref _errors);
            }

            public void IncrementNodesAdded()
            {
                Interlocked.Increment(ref _nodesAdded);
            }

            public void IncrementNodesRemoved()
            {
                Interlocked.Increment(ref _nodesRemoved);
            }
        }

        private const string COUCHBASE_CONFIG_TEMPLATE =
            @"{
""name"":""Some.Bucket"",
""bucketType"":""membase"",
""authType"":""none"",
""saslPassword"":"""",
""proxyPort"":11220,
""replicaIndex"":false,
""uri"":""/pools/default/buckets/Some.Bucket?bucket_uuid=a923c4eeaacf1c926725a3276a4975c0"",
""streamingUri"":""/pools/default/bucketsStreaming/Some.Bucket?bucket_uuid=a923c4eeaacf1c926725a3276a4975c0"",
""localRandomKeyUri"":""/pools/default/buckets/Some.Bucket/localRandomKey"",
""controllers"":{""compactAll"":""/pools/default/buckets/Some.Bucket/controller/compactBucket"",""compactDB"":""/pools/default/buckets/default/controller/compactDatabases"",""purgeDeletes"":""/pools/default/buckets/Some.Bucket/controller/unsafePurgeBucket"",""startRecovery"":""/pools/default/buckets/Some.Bucket/controller/startRecovery""},
""nodes"":
[
  {
    ""couchApiBase"":""http://127.0.0.1:9000/Some.Bucket"",
    ""systemStats"":{""cpu_utilization_rate"":1.093983092988563,""swap_total"":0,""swap_used"":0,""mem_total"":202931707904,""mem_free"":183185842176},
    ""interestingStats"":{""cmd_get"":0.0,""couch_docs_actual_disk_size"":13753440844,""couch_docs_data_size"":11327347770,""couch_views_actual_disk_size"":221166361,""couch_views_data_size"":211321639,""curr_items"":13059048,""curr_items_tot"":19484333,""ep_bg_fetched"":0.0,""get_hits"":0.0,""mem_used"":14144082736,""ops"":0.0,""vb_replica_curr_items"":6425285},
    ""uptime"":""7597460"",""memoryTotal"":202931707904,""memoryFree"":183185842176,""mcdMemoryReserved"":154824,""mcdMemoryAllocated"":154824,
    ""replication"":1.0,""clusterMembership"":""active"",""status"":""healthy"",""otpNode"":""ns_1@127.0.0.1"",""hostname"":""127.0.0.1:9000"",""clusterCompatibility"":131072,
    ""version"":""2.2.0-837-rel-enterprise"",""os"":""x86_64-unknown-linux-gnu"",
    ""ports"":{""proxy"":11211,""direct"":11210}
  },
  {
    ""couchApiBase"":""http://127.0.0.2:9001/Some.Bucket"",
    ""systemStats"":{""cpu_utilization_rate"":5.63613758806465,""swap_total"":0,""swap_used"":0,""mem_total"":202931707904,""mem_free"":183238750208},
    ""interestingStats"":{""cmd_get"":0.0,""couch_docs_actual_disk_size"":14799326216,""couch_docs_data_size"":11323369580,""couch_views_actual_disk_size"":217004825,""couch_views_data_size"":210620881,""curr_items"":13053434,""curr_items_tot"":19480448,""ep_bg_fetched"":0.0,""get_hits"":0.0,""mem_used"":14140751216,""ops"":0.0,""vb_replica_curr_items"":6427014},
    ""uptime"":""7597448"",""memoryTotal"":202931707904,""memoryFree"":183238750208,""mcdMemoryReserved"":154824,""mcdMemoryAllocated"":154824,
    ""replication"":1.0,""clusterMembership"":""active"",""status"":""healthy"",""otpNode"":""ns_2@127.0.0.2"",""thisNode"":true,""hostname"":""127.0.0.2:9001"",
    ""clusterCompatibility"":131072,""version"":""2.2.0-837-rel-enterprise"",""os"":""x86_64-unknown-linux-gnu"",
    ""ports"":{""proxy"":11213,""direct"":11212}}
],
""stats"":{""uri"":""/pools/default/buckets/Some.Bucket/stats"",""directoryURI"":""/pools/default/buckets/Some.Bucket/statsDirectory"",""nodeStatsListURI"":""/pools/default/buckets/Some.Bucket/nodes""},
""ddocs"":{""uri"":""/pools/default/buckets/Some.Bucket/ddocs""},
""nodeLocator"":""vbucket"",
""fastWarmupSettings"":false,""autoCompactionSettings"":false,""uuid"":""a923c4eeaacf1c926725a3276a4975c0"",
""vBucketServerMap"":
{
  ""hashAlgorithm"":""CRC"",
  ""numReplicas"":1,
  ""serverList"":[__SERVER_LIST__],
  ""vBucketMap"":[[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1]]
},
""replicaNumber"":1,
""threadsNumber"":3,
""quota"":{""ram"":128849018880,""rawRAM"":64424509440},
""basicStats"":{""quotaPercentUsed"":11.857786203424135,""opsPerSec"":0.0,""diskFetches"":0.0,""itemCount"":6310828,""diskUsed"":15348628661,""dataUsed"":11858064522,""memUsed"":15278641184},
""bucketCapabilitiesVer"":"""",
""bucketCapabilities"":[""touch"",""couchapi""]
}";

        private TestCounters _counters;
        private CouchbaseCluster _cluster;
        private ManualResetEventSlim _mreConfig;

        [SetUp]
        public void TestSetUp()
        {
            _counters = new TestCounters();
            _mreConfig = new ManualResetEventSlim();
            _cluster = new CouchbaseCluster(new MemcacheClientConfiguration() { TransportConnectTimerPeriod = Timeout.InfiniteTimeSpan }, "Some.Bucket", new[] { new IPEndPoint(0, 0) });
            _cluster.NodeAdded += _ => _counters.IncrementNodesAdded();
            _cluster.NodeRemoved += _ => _counters.IncrementNodesRemoved();
            _cluster.OnError += e => { _counters.IncrementErrors(); Console.Error.WriteLine(e.Message); Console.Error.WriteLine(e.StackTrace); };
            _cluster.OnConfig += () => _mreConfig.Set();
        }

        [TearDown]
        public void TestTearDown()
        {
            _cluster.Dispose();
            _cluster = null;

            _mreConfig.Dispose();
            _mreConfig = null;
        }

        [Test]
        public void TestInitialConfigurationUpdate()
        {
            _mreConfig.Reset();
            _counters.Reset();

            // Send a config with two nodes
            _cluster.HandleConfigurationUpdate(GetValidConfig(new List<string>() { "127.0.0.1:11210", "127.0.0.2:11212" }));

            Assert.IsTrue(_mreConfig.Wait(TimeSpan.FromSeconds(2)), "The Couchbase cluster failed to receive the new config");

            Assert.AreEqual(2, _counters.NodesAdded, "nodes added");
            Assert.AreEqual(0, _counters.NodesRemoved, "nodes removed");
            Assert.AreEqual(0, _counters.Errors, "errors");

            var locator = _cluster.Locator as VBucketServerMapLocator;
            Assert.IsNotNull(locator);

            Assert.AreEqual(2, locator.Nodes.Count, "number of nodes");
            Assert.AreEqual(1024, locator.VBucketMap.Length);
            Assert.AreEqual(2, locator.VBucketMap[0].Length);
        }

        [Test]
        public void TestNoopConfigurationUpdate()
        {
            // Initialize + config with two nodes
            TestInitialConfigurationUpdate();

            _mreConfig.Reset();
            _counters.Reset();

            // Send the same conf again, expect no change in the cluster
            _cluster.HandleConfigurationUpdate(GetValidConfig(new List<string>() { "127.0.0.1:11210", "127.0.0.2:11212" }));

            Assert.IsTrue(_mreConfig.Wait(TimeSpan.FromSeconds(2)), "The Couchbase cluster failed to receive the new config");

            Assert.AreEqual(0, _counters.NodesAdded, "nodes added");
            Assert.AreEqual(0, _counters.NodesRemoved, "nodes removed");
            Assert.AreEqual(0, _counters.Errors, "errors");
        }

        [Test]
        public void TestConfigurationSeveralUpdates()
        {
            _mreConfig.Reset();
            _counters.Reset();

            // Send a config with one node
            _cluster.HandleConfigurationUpdate(GetValidConfig(new List<string>() { "127.0.0.1:11210" }));

            Assert.IsTrue(_mreConfig.Wait(TimeSpan.FromSeconds(2)), "The Couchbase cluster failed to receive the new config (1)");

            Assert.AreEqual(1, _counters.NodesAdded, "nodes added");
            Assert.AreEqual(0, _counters.NodesRemoved, "nodes removed");
            Assert.AreEqual(0, _counters.Errors, "errors");

            var locator = _cluster.Locator as VBucketServerMapLocator;
            Assert.IsNotNull(locator);
            Assert.AreEqual(1, locator.Nodes.Count, "number of nodes");

            _mreConfig.Reset();
            _counters.Reset();

            // Send a config with another node (2 nodes total)
            _cluster.HandleConfigurationUpdate(GetValidConfig(new List<string>() { "127.0.0.1:11210", "127.0.0.2:11212" }));

            Assert.IsTrue(_mreConfig.Wait(TimeSpan.FromSeconds(2)), "The Couchbase cluster failed to receive the new config (2)");

            Assert.AreEqual(1, _counters.NodesAdded, "nodes added");
            Assert.AreEqual(0, _counters.NodesRemoved, "nodes removed");
            Assert.AreEqual(0, _counters.Errors, "errors");

            locator = _cluster.Locator as VBucketServerMapLocator;
            Assert.IsNotNull(locator);
            Assert.AreEqual(2, locator.Nodes.Count, "number of nodes");

            _mreConfig.Reset();
            _counters.Reset();

            // Remove one node
            _cluster.HandleConfigurationUpdate(GetValidConfig(new List<string>() { "127.0.0.2:11212" }));

            Assert.IsTrue(_mreConfig.Wait(TimeSpan.FromSeconds(2)), "The Couchbase cluster failed to receive the new config (3)");

            Assert.AreEqual(0, _counters.NodesAdded, "nodes added");
            Assert.AreEqual(1, _counters.NodesRemoved, "nodes removed");
            Assert.AreEqual(0, _counters.Errors, "errors");

            locator = _cluster.Locator as VBucketServerMapLocator;
            Assert.IsNotNull(locator);
            Assert.AreEqual(1, locator.Nodes.Count, "number of nodes");
        }

        private string GetValidConfig(IList<string> servers)
        {
            if (servers != null && servers.Count > 0)
                return COUCHBASE_CONFIG_TEMPLATE.Replace("__SERVER_LIST__", '"' + string.Join("\",\"", servers) + '"');
            else
                return COUCHBASE_CONFIG_TEMPLATE.Replace("__SERVER_LIST__", string.Empty);
        }
    }
}