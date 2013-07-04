using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Criteo.Memcache.Node;

namespace Criteo.Memcache.Locator
{
    public interface INodeLocator
    {
        /// <summary>
        /// This method is called every time a change occures on the node list
        /// </summary>
        /// <param name="nodes">The list of nodes</param>
        void Initialize(IList<IMemcacheNode> nodes);

        /// <summary>
        /// This method should return the node where belongs the key or null if the they're all dead
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        IEnumerable<IMemcacheNode> Locate(string key);
    }
}
