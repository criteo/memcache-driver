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
using System.Threading;

using Criteo.Memcache.Cluster.Couchbase;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Exceptions;
using Criteo.Memcache.Locator;
using Criteo.Memcache.Node;
using ServiceStack.Text;

namespace Criteo.Memcache.Cluster
{
    /// <summary>
    /// Implements a dynamically managed memcached cluster which streams configuration and updates from
    /// CouchBase servers in order to remain consistent.
    /// </summary>
    internal class CouchbaseCluster : IMemcacheCluster
    {
        private bool _isInitialized;
        private bool _isDisposed = false;

        private readonly MemcacheClientConfiguration _configuration;
        private readonly string _bucket;

        private int _currentConfigurationHost;
        private readonly IPEndPoint[] _configurationHosts;

        private IDictionary<string, IMemcacheNode> _memcacheNodes;
        private INodeLocator _locator;

        private readonly Timer _connectionTimer;
        private AsyncLinesStreamReader _linesStreamReader;
        private WebResponse _webResponse;

        private readonly TimeSpan _initializationTimeout;

        public event Action<Exception> OnError;

        public event Action OnConfig;

        public CouchbaseCluster(MemcacheClientConfiguration configuration, string bucket, TimeSpan initializationTimeout, IPEndPoint[] configurationHosts)
        {
            if (configurationHosts.Length == 0)
                throw new ArgumentException("There should be at least one value in the list", "configurationHosts");

            _isInitialized = false;

            _linesStreamReader = null;
            _webResponse = null;

            _configuration = configuration;
            if (_configuration.Authenticator == null)
                _configuration.Authenticator = MemcacheClientConfiguration.SaslPlainAuthenticatorFactory(string.Empty, bucket, string.Empty);

            _bucket = bucket;

            _initializationTimeout = initializationTimeout;

            _currentConfigurationHost = 0;
            _configurationHosts = configurationHosts;

            _memcacheNodes = new Dictionary<string, IMemcacheNode>();

            _locator = null;

            _connectionTimer = new Timer(_ => ConnectToConfigurationStream(), null, Timeout.Infinite, Timeout.Infinite);
        }

        #region IMemcacheCluster

        public INodeLocator Locator { get { return _locator; } }

        public IEnumerable<IMemcacheNode> Nodes
        {
            get
            {
                return _memcacheNodes.Values.ToArray();
            }
        }

        public event Action<IMemcacheNode> NodeAdded;

        public event Action<IMemcacheNode> NodeRemoved;

        public void Initialize()
        {
            if (_isInitialized)
                throw new MemcacheException("Initialize should only be called once on clusters");

            SynchronouslyConnectConfiguration();
            ConnectToConfigurationStream();
            _isInitialized = true;
        }

        #endregion

        #region Couchbase HTTP configuration stream handling

        private void SynchronouslyConnectConfiguration()
        {
            var rand = new Random();
            var i = rand.Next(_configurationHosts.Length - 1);
            _currentConfigurationHost = (_currentConfigurationHost + i) % _configurationHosts.Length;

            var url = string.Format(
                "http://{0}:{1}/pools/default/buckets/{2}",
                _configurationHosts[_currentConfigurationHost].Address,
                _configurationHosts[_currentConfigurationHost].Port,
                _bucket);

            try
            {
                var webRequest = WebRequest.Create(url);
                webRequest.Timeout = (int)_initializationTimeout.TotalMilliseconds;
                var response = webRequest.GetResponse();
                using (var respStream = response.GetResponseStream())
                {
                    var reader = new StreamReader(respStream);
                    var chunk = reader.ReadToEnd();
                    HandleConfigurationUpdate(chunk);
                }
            }
            catch (Exception e)
            {
                OnError(e);
                throw;
            }
        }

        private void ConnectToConfigurationStream()
        {
            KillCurrentConnection();

            // Start loop at 1 to make sure we always try a server different from the last one upon (re)connecting.
            var connecting = false;
            for (var i = 1; !connecting && i <= _configurationHosts.Length; i++)
            {
                connecting = true;
                _currentConfigurationHost = (_currentConfigurationHost + i) % _configurationHosts.Length;

                var url = string.Format(
                    "http://{0}:{1}/pools/default/bucketsStreaming/{2}",
                    _configurationHosts[_currentConfigurationHost].Address,
                    _configurationHosts[_currentConfigurationHost].Port,
                    _bucket);

                var request = WebRequest.Create(url);
                request.Timeout = Timeout.Infinite;
                try
                {
                    request.BeginGetResponse(ConfigurationStreamRequestEndGetResponse, request);
                }
                catch (Exception e)
                {
                    // Try the next host
                    connecting = false;
                    if (OnError != null)
                        OnError(e);
                }
            }

            // In case no host can be reached, try again in 30s
            if (!connecting)
                RetryConnectingToConfigurationStream(delaySeconds: 30.0);
        }

        private void RetryConnectingToConfigurationStream(double delaySeconds)
        {
            lock (this)
                if (!_isDisposed)
                    _connectionTimer.Change(TimeSpan.FromSeconds(delaySeconds), TimeSpan.FromMilliseconds(Timeout.Infinite));
        }

        private void ConfigurationStreamRequestEndGetResponse(IAsyncResult ar)
        {
            if (_linesStreamReader != null)
            {
                OnError(new MemcacheException("Sanity check: The previous AsyncLinesStreamReader was not disposed"));
                return;
            }
            if (_webResponse != null)
            {
                OnError(new MemcacheException("Sanity check: The previous WebResponse was not disposed"));
                return;
            }

            WebResponse response = null;
            AsyncLinesStreamReader linesStreamReader = null;

            try
            {
                var request = (WebRequest)ar.AsyncState;
                response = request.EndGetResponse(ar);

                // Allocate new string reader and start reading
                linesStreamReader = new AsyncLinesStreamReader(response.GetResponseStream());
                linesStreamReader.OnError += HandleLinesStreamError;
                linesStreamReader.OnChunk += HandleConfigurationUpdate;

                lock (this)
                    if (!_isDisposed)
                    {
                        _webResponse = response;
                        _linesStreamReader = linesStreamReader;
                    }
                    else
                    {
                        response.Dispose();
                        linesStreamReader.Dispose();
                        return;
                    }

                _linesStreamReader.StartReading();
            }
            catch (Exception e)
            {
                if (OnError != null)
                    OnError(e);

                if (response != null)
                    response.Dispose();
                _webResponse = null;

                if (linesStreamReader != null)
                    linesStreamReader.Dispose();
                _linesStreamReader = null;

                // Handle HTTP query errors by trying another host
                RetryConnectingToConfigurationStream(delaySeconds: 1.0);
            }
        }

        /// <summary>
        /// Handles chunks of data, by parsing them as JSON and updating the current cluster state.
        /// </summary>
        /// <param name="chunk">Chunk of data</param>
        internal void HandleConfigurationUpdate(string chunk)
        {
            try
            {
                var bucket = JsonSerializer.DeserializeFromString<JsonBucket>(chunk);
                if (bucket == null)
                    throw new ConfigurationException("Received an empty bucket configuration from Couchbase for bucket " + _bucket);

                // Serialize configuration updates to avoid trouble
                lock (this)
                {
                    IList<string> nodesEndPoint;
                    var nodeStatus = GetNodesFromConfiguration(bucket);
                    List<IMemcacheNode> toBeDeleted;

                    switch (bucket.NodeLocator)
                    {
                        case JsonBucket.TYPE_VBUCKET:
                            {
                                if (bucket.VBucketServerMap == null || bucket.VBucketServerMap.VBucketMap == null || bucket.VBucketServerMap.ServerList == null)
                                    throw new ConfigurationException("Received an empty vbucket map from Couchbase for bucket " + _bucket);

                                // The endpoints in VBucketServerMap are in the right order
                                nodesEndPoint = bucket.VBucketServerMap.ServerList;
                                var updatedNodes = GenerateUpdatedNodeList(nodesEndPoint, nodeStatus, out toBeDeleted);
                                // Atomic update to the latest cluster state
                                _locator = new VBucketServerMapLocator(updatedNodes, bucket.VBucketServerMap.VBucketMap);
                                break;
                            }
                        case JsonBucket.TYPE_KETAMA:
                            {
                                if (_locator == null)
                                    _locator = new KetamaLocator();

                                // create new nodes
                                nodesEndPoint = nodeStatus.Keys.ToList();
                                var updatedNodes = GenerateUpdatedNodeList(nodesEndPoint, nodeStatus, out toBeDeleted);
                                _locator.Initialize(updatedNodes);
                                break;
                            }
                        default:
                            throw new ConfigurationException("Unhandled locator type: " + bucket.NodeLocator);
                    }

                    // Dispose of the unused nodes after updating the current state
                    CleanupNodes(toBeDeleted);
                }
            }
            catch (Exception e)
            {
                if (OnError != null)
                    OnError(e);
            }
            finally
            {
                if (OnConfig != null)
                    OnConfig();
            }
        }

        private static IDictionary<string, bool> GetNodesFromConfiguration(JsonBucket bucket)
        {
            var nodes = new Dictionary<string, bool>();
            foreach (var node in bucket.Nodes)
            {
                // the hostnames includes the port…
                if (node.ClusterMembership.Equals("active", StringComparison.OrdinalIgnoreCase))
                {
                    var nodeHostname = node.Hostname.Split(':').FirstOrDefault();
                    var nodePort = node.Ports.Direct;
                    var isReachable = node.Status == JsonNode.STATUS_HEALTHY || node.Status == JsonNode.STATUS_WARMUP;
                    nodes[nodeHostname + ':' + nodePort] = isReachable;
                }
            }
            return nodes;
        }

        /// <summary>
        /// Update and return the ordered list of memcache nodes in the cluster. Existing nodes are kept from the previous list.
        /// </summary>
        /// <param name="activeNodes">Node addresses. The order of this list is maintained in the list returned by the method.</param>
        /// <param name="nodeStatus">[Optional] Associative map of the nodes and their status (reachable/not reachable)</param>
        /// <param name="toBeDeleted">Returns the nodes from the previous configuration that need to be deleted</param>
        /// <returns></returns>
        private IList<IMemcacheNode> GenerateUpdatedNodeList(ICollection<string> activeNodes, IDictionary<string, bool> nodeStatus, out List<IMemcacheNode> toBeDeleted)
        {
            var oldNodes = new Dictionary<string, IMemcacheNode>(_memcacheNodes);
            var newNodes = new List<KeyValuePair<string, IMemcacheNode>>(activeNodes.Count);
            toBeDeleted = new List<IMemcacheNode>();

            foreach (var server in activeNodes)
            {
                // The node is considered healthy unless stated otherwise in the nodeStatus argument
                bool isReachable = nodeStatus == null || !nodeStatus.ContainsKey(server) || nodeStatus[server];

                IMemcacheNode node;
                if (oldNodes.TryGetValue(server, out node))
                {
                    bool previousIsReachable = !(node is UnhealthyNode);
                    if (isReachable != previousIsReachable)
                    {
                        // Healthy status has changed for this node. Replace it.
                        toBeDeleted.Add(node);
                        node = null;
                    }
                }

                // The node did not exist or must be replaced
                if (node == null)
                {
                    node = ClusterNodeFactory(server, isReachable);
                    if (NodeAdded != null)
                        NodeAdded(node);
                }

                newNodes.Add(new KeyValuePair<string, IMemcacheNode>(server, node));
            }

            // Prepare clean-up of removed nodes
            foreach (var removedNode in oldNodes.Keys.Except(activeNodes))
                toBeDeleted.Add(oldNodes[removedNode]);

            // Update the node configuration and return an ordered list
            _memcacheNodes = newNodes.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            return newNodes.Select(kvp => kvp.Value).ToList();
        }

        private IMemcacheNode ClusterNodeFactory(string server, bool isHealthy)
        {
            var parts = server.SplitOnFirst(':');
            var ip = IPAddress.Parse(parts[0]);
            var port = int.Parse(parts[1]);
            var endpoint = new IPEndPoint(ip, port);

            if (isHealthy)
            {
                var nodeFactory = _configuration.NodeFactory ?? MemcacheClientConfiguration.DefaultNodeFactory;
                return nodeFactory(endpoint, _configuration);
            }
            else
            {
                return new UnhealthyNode(endpoint);
            }
        }

        private void CleanupNodes(IEnumerable<IMemcacheNode> toBeDeleted)
        {
            if (toBeDeleted == null)
                return;

            foreach (var node in toBeDeleted)
            {
                // Nodes which get deleted either should not or cannot receive any requests,
                // and as such forcing the shutdown (thus marking any pending request as failed)
                // is probably the most coherent way to handle things.
                node.Shutdown(node.Dispose);

                if (NodeRemoved != null)
                    NodeRemoved(node);
            }
        }

        /// <summary>
        /// Handles errors which were caught by the asynchronous bucket stream reader
        /// by disposing of it and trying to connect to another CouchBase server.
        /// </summary>
        /// <param name="err">The exception which was caught upon trying to read data</param>
        private void HandleLinesStreamError(Exception err)
        {
            RetryConnectingToConfigurationStream(delaySeconds: 1.0);
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            lock (this)
            {
                _isDisposed = true;

                if (disposing)
                {
                    _connectionTimer.Dispose();

                    KillCurrentConnection();

                    foreach (var node in _memcacheNodes.Values)
                        node.Dispose();
                }
            }
        }

        private void KillCurrentConnection()
        {
            if (_linesStreamReader != null)
            {
                _linesStreamReader.Dispose();
                _linesStreamReader = null;
            }

            if (_webResponse != null)
            {
                _webResponse.Dispose();
                _webResponse = null;
            }
        }

        #endregion
    }
}