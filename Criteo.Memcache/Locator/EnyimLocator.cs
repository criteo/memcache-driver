using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Node;

namespace Criteo.Memcache.Locator
{
    internal class EnyimLocator : INodeLocator
    {
        public void Initialize(IList<IMemcacheNode> nodes)
        {
            throw new NotImplementedException();
        }

        public IMemcacheNode Locate(string key)
        {
            throw new NotImplementedException();
        }
    }
}
