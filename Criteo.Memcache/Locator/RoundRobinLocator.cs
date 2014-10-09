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
using System.Collections.Generic;
using System.Threading;

using Criteo.Memcache.Node;

namespace Criteo.Memcache.Locator
{
    internal class RoundRobinLocator : INodeLocator
    {
        private IList<IMemcacheNode> _nodes;
        private int _lastPosition = 0;

        public void Initialize(IList<IMemcacheNode> nodes)
        {
            _nodes = nodes;
        }

        public IEnumerable<IMemcacheNode> Locate(byte[] key)
        {
            int position = Interlocked.Increment(ref _lastPosition) % _nodes.Count;
            position = position >= 0 ? position : position + _nodes.Count;

            for(int i = 0; i < _nodes.Count; ++i)
            {
                var selectedNode = _nodes[position];
                if (!selectedNode.IsDead)
                    yield return selectedNode;

                position++;
                if (position >= _nodes.Count)
                    position = 0;
            }
        }
    }
}
