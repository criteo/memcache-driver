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
