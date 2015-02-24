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
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Util;

namespace Criteo.Memcache.Locator
{
    /// <summary>
    /// Implements couchbase's vBucket map based key locator.
    /// </summary>
    internal class VBucketServerMapLocator : INodeLocator
    {
        private const uint HashMask = (1 << 10) - 1;
        private static readonly VBucketHash Hash = new VBucketHash();

        public readonly IList<IMemcacheNode> Nodes;
        public readonly int[][] VBucketMap;

        public VBucketServerMapLocator(IList<IMemcacheNode> nodes, int[][] vBucketMap)
        {
            Nodes = nodes;
            VBucketMap = vBucketMap;
        }

        public void Initialize(IList<IMemcacheNode> nodes)
        {
            throw new NotImplementedException("Initialize should not be manually called on the CouchBase VBucket locator");
        }

        public IEnumerable<IMemcacheNode> Locate(IMemcacheRequest req)
        {
            // Compute the vBucket's index using a modified CRC32
            var vBucketIndex = Hash.Compute(req.Key) & HashMask;
            var couchbaseReq = req as ICouchbaseRequest;
            if (couchbaseReq != null)
                couchbaseReq.VBucket = (ushort)vBucketIndex;

            // the call to the yield is done in another method in order to
            // have the VBucket changed before we start iterating over the
            // located nodes
            return YieldingLocate(req, vBucketIndex);
        }

        private IEnumerable<IMemcacheNode> YieldingLocate(IMemcacheRequest req, uint vBucketIndex)
        {
            // Check for non-configured, badly configured, or dead cluster
            if (Nodes == null || Nodes.Count == 0 || VBucketMap == null || VBucketMap.Length != 1024)
                yield break;

            // Yield replica in the same order (the first one being the master)
            foreach (var node in VBucketMap[vBucketIndex])
                if (0 <= node && node < Nodes.Count)
                    yield return Nodes[node];
                else
                    continue;
        }
    }
}
