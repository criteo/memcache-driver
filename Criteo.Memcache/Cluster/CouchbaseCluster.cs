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

        private readonly MemcacheClientConfiguration _configuration;
        private readonly string _bucket;

        private int _currentConfigurationHost;
        private readonly IPEndPoint[] _configurationHosts;

        private readonly IDictionary<string, IMemcacheNode> _memcacheNodes;
        private VBucketServerMapLocator _locator;

        private readonly Timer _connectionTimer;
        private readonly ManualResetEventSlim _receivedInitialConfigurationBarrier;
        private AsyncLinesStreamReader _linesStreamReader;

        public event Action<Exception> OnError;

        public CouchbaseCluster(MemcacheClientConfiguration configuration, string bucket, IPEndPoint[] configurationHosts)
        {
            if (configurationHosts.Length == 0)
                throw new ArgumentException("There should be at least one value in the list", "configurationHosts");

            _isInitialized = false;

            _configuration = configuration;
            if (_configuration.Authenticator == null)
                _configuration.Authenticator = MemcacheClientConfiguration.SaslPlainAuthenticatorFactory(string.Empty, bucket, string.Empty);

            _bucket = bucket;

            _currentConfigurationHost = 0;
            _configurationHosts = configurationHosts;

            _memcacheNodes = new Dictionary<string, IMemcacheNode>();

            _locator = new VBucketServerMapLocator(new List<IMemcacheNode>(), new int[][] { });

            _connectionTimer = new Timer(_ => ConnectToConfigurationStream(), null, Timeout.Infinite, Timeout.Infinite);
            _receivedInitialConfigurationBarrier = new ManualResetEventSlim();
        }

        #region IMemcacheCluster

        public INodeLocator Locator { get { return _locator; } }
        public IEnumerable<IMemcacheNode> Nodes { get { return _locator.Nodes; } }

        public event Action<IMemcacheNode> NodeAdded;
        public event Action<IMemcacheNode> NodeRemoved;

        public void Initialize()
        {
            if (_isInitialized)
                throw new MemcacheException("Initialize should only be called once on clusters");

            _connectionTimer.Change(0, Timeout.Infinite);

            // Wait till we either get the initial configuration or timeout
            if (!_receivedInitialConfigurationBarrier.Wait(_configuration.TransportConnectTimerPeriod))
                throw new TimeoutException("Attempts to connect to CouchBase nodes resulted in a timeout");

            _isInitialized = true;
        }

        #endregion

        #region Couchbase HTTP configuration stream handling

        private void ConnectToConfigurationStream()
        {
            if (_linesStreamReader != null)
            {
                _linesStreamReader.Dispose();
                _linesStreamReader = null;
            }

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
                RetryConnectingToConfigurationStream(30);
        }

        private void RetryConnectingToConfigurationStream(double delaySeconds)
        {
            _connectionTimer.Change(TimeSpan.FromSeconds(delaySeconds), TimeSpan.FromMilliseconds(Timeout.Infinite));
        }

        private void ConfigurationStreamRequestEndGetResponse(IAsyncResult ar)
        {
            try
            {
                var request = (WebRequest)ar.AsyncState;
                var response = request.EndGetResponse(ar);

                // Allocate new string reader and start reading
                _linesStreamReader = new AsyncLinesStreamReader(response.GetResponseStream());
                _linesStreamReader.OnError += HandleLinesStreamError;
                _linesStreamReader.OnChunk += HandleConfigurationUpdate;

                _linesStreamReader.StartReading();
            }
            catch (Exception e)
            {
                if (OnError != null)
                    OnError(e);

                // Handle HTTP query errors by trying another host
                RetryConnectingToConfigurationStream(1);
            }
        }

        /// <summary>
        /// Handles chunks of data, by parsing them as JSON and updating the current cluster state.
        /// </summary>
        /// <param name="chunk">Chunk of data</param>
        public void HandleConfigurationUpdate(Stream chunk)
        {
            var bucket = JsonSerializer.DeserializeFromStream<JsonBucket>(chunk);
            if (bucket == null)
                return;

            var vBucketServerMap = bucket.VBucketServerMap;
            if (vBucketServerMap == null)
                return;

            // Serialize configuration updates to avoid trouble
            lock (this)
            {
                var updatedNodes = _locator.Nodes;
                if (vBucketServerMap.ServerList != null)
                    updatedNodes = GenerateUpdatedNodeList(vBucketServerMap.ServerList);

                // Atomic update to the latest cluster state
                var updatedVBucketMap = vBucketServerMap.VBucketMap ?? _locator.VBucketMap;

                _locator = new VBucketServerMapLocator(updatedNodes, updatedVBucketMap);

                // Dispose of the unused nodes after updating the current state
                if (vBucketServerMap.ServerList != null)
                    CleanupDeletedNodes(vBucketServerMap.ServerList);
            }

            _receivedInitialConfigurationBarrier.Set();
        }

        private List<IMemcacheNode> GenerateUpdatedNodeList(ICollection<string> activeNodes)
        {
            var updatedNodes = new List<IMemcacheNode>(activeNodes.Count);
            var nodeFactory = _configuration.NodeFactory ?? MemcacheClientConfiguration.DefaultNodeFactory;
            foreach (var server in activeNodes)
            {
                try
                {
                    IMemcacheNode node;
                    if (!_memcacheNodes.TryGetValue(server, out node))
                    {
                        var parts = server.SplitOnFirst(':');
                        var ip = IPAddress.Parse(parts[0]);
                        var port = int.Parse(parts[1]);
                        var endpoint = new IPEndPoint(ip, port);

                        node = nodeFactory(endpoint, _configuration);
                        _memcacheNodes.Add(server, node);

                        if (NodeAdded != null)
                            NodeAdded(node);
                    }

                    updatedNodes.Add(node);
                }
                catch (Exception e)
                {
                    if (OnError != null)
                        OnError(e);
                }
            }

            return updatedNodes;
        }

        private void CleanupDeletedNodes(IEnumerable<string> activeNodes)
        {
            foreach (var server in _memcacheNodes.Keys.Except(activeNodes))
            {
                var node = _memcacheNodes[server];
                _memcacheNodes.Remove(server);

                // Nodes which get deleted either should not or cannot receive any requests,
                // and as such forcing the shutdown (thus marking any pending request as failed)
                // is probably the most coherent way to handle things.
                node.Shutdown(true);
                node.Dispose();

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
            RetryConnectingToConfigurationStream(1);
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            _connectionTimer.Dispose();

            if (_linesStreamReader != null)
                _linesStreamReader.Dispose();
        }

        #endregion
    }
}
