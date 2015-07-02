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
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Locator;
using Criteo.Memcache.Node;

namespace Criteo.Memcache.Cluster
{
    internal class StaticCluster : IMemcacheCluster
    {
        private IList<IMemcacheNode> _nodes;
        private readonly MemcacheClientConfiguration _configuration;

        public INodeLocator Locator { get; private set; }

        public event Action<IMemcacheNode> NodeAdded;

        public event Action<IMemcacheNode> NodeRemoved;

        public event Action<Exception> OnError;

        public StaticCluster(MemcacheClientConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Initialize()
        {
            _nodes = new List<IMemcacheNode>(_configuration.NodesEndPoints.Count);

            foreach (var nodeEndPoint in _configuration.NodesEndPoints)
            {
                var node = (_configuration.NodeFactory ?? MemcacheClientConfiguration.DefaultNodeFactory)(nodeEndPoint, _configuration);
                _nodes.Add(node);
                if (NodeAdded != null)
                    NodeAdded(node);
            }

            var locator = (_configuration.NodeLocatorFactory ?? MemcacheClientConfiguration.DefaultLocatorFactory)();
            locator.Initialize(_nodes);
            Locator = locator;
        }

        public IEnumerable<IMemcacheNode> Nodes
        {
            get { return _nodes; }
        }

        #region IDisposable
        private bool _disposed;
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                    foreach (var node in _nodes)
                        node.Dispose();
                _disposed = true;
            }
        }
        #endregion IDisposable
    }
}
