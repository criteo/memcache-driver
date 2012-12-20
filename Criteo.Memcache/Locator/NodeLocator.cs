using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Criteo.Memcache.Node;

namespace Criteo.Memcache.Locator
{
    internal class RoundRobinNodeLocator : INodeLocator
    {
        private int _lastPosition = 0;
        public IMemcacheNode Locate(string key, IList<IMemcacheNode> nodes)
        {
            for(int i=0; i<nodes.Count; ++i)
            {
                var position = Interlocked.Increment(ref _lastPosition) % nodes.Count;
                position = position >= 0 ? position : position + nodes.Count;
                var selectedNode = nodes[position];
                if (!selectedNode.IsDead)
                    return selectedNode;
            }

            return null;
        }
    }
}
