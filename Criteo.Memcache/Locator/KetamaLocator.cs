using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.Locator
{
    /// <summary>
    /// Basically very inspired (and copy/paste) from Enyim.Caching
    /// </summary>
    internal class KetamaLocator : INodeLocator
    {
        private const string DefaultHashName = "System.Security.Cryptography.MD5";
        private const int ServerAddressMutations = 160;

        private readonly HashPool _hashPool;
        private IList<IMemcacheNode> _nodes;
        private LookupData _lookupData;

        readonly Action<IMemcacheNode> _nodeStateChange;

        public KetamaLocator(string hashName = null)
        {
            _hashPool = new HashPool(() => HashAlgorithm.Create(hashName ?? DefaultHashName));
            _nodeStateChange = _ => Reinitialize();
        }

        public void Initialize(IList<IMemcacheNode> nodes)
        {
            if (nodes == null)
                throw new ArgumentNullException("nodes");

            if (_nodes != null)
            {
                // first de-register events on removed nodes
                foreach (var node in _nodes.Except(nodes))
                {
                    node.NodeDead -= _nodeStateChange;
                    node.NodeAlive -= _nodeStateChange;
                }

                // then register only the new ones
                foreach (var node in nodes.Except(_nodes))
                {
                    node.NodeDead += _nodeStateChange;
                    node.NodeAlive += _nodeStateChange;
                }
            }
            else
            {
                foreach (var node in nodes)
                {
                    node.NodeDead += _nodeStateChange;
                    node.NodeAlive += _nodeStateChange;
                }
            }
            _nodes = nodes;

            using (var hash = _hashPool.Hash)
                _lookupData = new LookupData(nodes, hash.Value);

            UpdateState();
        }

        private void Reinitialize()
        {
            // filter only working nodes and
            var newNodes = new List<IMemcacheNode>(_nodes.Count);
            newNodes.AddRange(_nodes.Where(node => !node.IsDead));

            using (var hash = _hashPool.Hash)
                Interlocked.Exchange(ref _lookupData, new LookupData(newNodes, hash.Value));
        }

        /// <summary>
        /// Checks if the liveness of the nodes has changed
        /// </summary>
        private void UpdateState()
        {
            var ld = _lookupData;
            var nodeSet = new HashSet<IMemcacheNode>(ld.Nodes);

            // Check for nodes which are either:
            // - Dead and still in the active node list
            // - Alive and not yet in the active node list
            if (_nodes.Any(node => node.IsDead == nodeSet.Contains(node)))
                Reinitialize();
        }

        private uint GetKeyHash(byte[] key)
        {
            using (var hash = _hashPool.Hash)
                return hash.Value.ComputeHash(key).CopyToUIntNoRevert(0);
        }

        public IEnumerable<IMemcacheNode> Locate(IMemcacheRequest req)
        {
            var key = req.Key;
            if (key == null)
                throw new ArgumentNullException("key");

            var ld = _lookupData;
            if (ld.Nodes.Length == 0)
            {
                yield break;
            }
            if (ld.Nodes.Length == 1)
            {
                var firstNode = ld.Nodes[0];
                if (!firstNode.IsDead)
                    yield return firstNode;
                yield break;
            }

            // Return alive JsonNodes only.
            foreach (var retNode in LocateNode(ld, GetKeyHash(key)))
            {
                if (retNode != null && !retNode.IsDead)
                {
                    yield return retNode;
                }
            }
        }

        /// <summary>
        /// Enumerate server nodes in the order they appear in the consistent hashing table.
        /// </summary>
        /// <param name="ld">Consistent hashing lookup table</param>
        /// <param name="itemKeyHash">Hash key used to identify the first server node to be returned by the function</param>
        /// <returns></returns>
        private static IEnumerable<IMemcacheNode> LocateNode(LookupData ld, uint itemKeyHash)
        {
            // get the index of the server assigned to this hash
            int foundIndex = Array.BinarySearch(ld.SortedKeys, itemKeyHash);
            IMemcacheNode node1 = null;
            IMemcacheNode node2 = null;
            HashSet<IMemcacheNode> usedNodes = null;

            // no exact match
            if (foundIndex < 0)
            {
                // this is the next greater index in the list
                foundIndex = ~foundIndex;

                if (foundIndex >= ld.SortedKeys.Length)
                {
                    // the key was larger than all server keys, so return the first server
                    foundIndex = 0;
                }
            }

            // Paranoid check
            if (foundIndex < 0 || foundIndex >= ld.SortedKeys.Length)
                yield break;

            // Return distinct JsonNodes. Exit after a complete loop over the keys.
            int startingIndex = foundIndex;
            do
            {
                IMemcacheNode node = ld.KeyToServer[ld.SortedKeys[foundIndex]];
                if (node1 == null)
                {
                    node1 = node;
                    yield return node1;
                }
                else if (node2 == null)
                {
                    // Because the list of keys is filtered with function CleanRepeatedNodes, we have node2 != node1
                    node2 = node;
                    yield return node2;
                }
                else
                {
                    if (usedNodes == null)
                    {
                        usedNodes = new HashSet<IMemcacheNode> { node1, node2 };
                    }
                    if (!usedNodes.Contains(node))
                    {
                        usedNodes.Add(node);
                        yield return node;
                    }
                }

                foundIndex++;
                if (foundIndex >= ld.SortedKeys.Length)
                {
                    foundIndex = 0;
                }
            } while (foundIndex != startingIndex);
        }

        /// <summary>
        /// this will encapsulate all the indexes we need for lookup
        /// so the instance can be reinitialized without locking
        /// </summary>
        private class LookupData
        {
            // holds all server nodes. Order and size of this array is not consistent with array SortedKeys.
            public readonly IMemcacheNode[] Nodes;
            // holds all server keys for mapping an item key to the server consistently
            public readonly uint[] SortedKeys;
            // used to lookup a server based on its key
            public readonly Dictionary<uint, IMemcacheNode> KeyToServer;

            public LookupData(IList<IMemcacheNode> nodes, HashAlgorithm hash)
            {
                int partCount = hash.HashSize / (8 * sizeof(uint)); // HashSize is in bits, uint is 4 bytes long
                if (partCount < 1)
                    throw new ArgumentOutOfRangeException("hash", "The hash algorithm must provide at least 32 bits long hashes");

                var keys = new List<uint>(nodes.Count);
                KeyToServer = new Dictionary<uint, IMemcacheNode>(nodes.Count, new UIntEqualityComparer());

                foreach (var currentNode in nodes)
                {
                    // every server is registered numberOfKeys times
                    // using UInt32s generated from the different parts of the hash
                    // i.e. hash is 64 bit:
                    // 01 02 03 04 05 06 07
                    // server will be stored with keys 0x07060504 & 0x03020100
                    string address = currentNode.EndPoint.ToString();

                    for (int mutation = 0; mutation < ServerAddressMutations / partCount; mutation++)
                    {
                        byte[] data = hash.ComputeHash(Encoding.ASCII.GetBytes(address + "-" + mutation));
                        for (int p = 0; p < partCount; p++)
                        {
                            var key = data.CopyToUIntNoRevert(p * 4);
                            keys.Add(key);
                            KeyToServer[key] = currentNode;
                        }
                    }
                }

                keys.Sort();
                SortedKeys = CleanRepeatedNodes(keys, KeyToServer);
                Nodes = nodes.ToArray();
            }
        }

        /// <summary>
        /// A fast comparer for dictionaries indexed by UInt. Faster than using Comparer.Default
        /// </summary>
        private sealed class UIntEqualityComparer : IEqualityComparer<uint>
        {
            bool IEqualityComparer<uint>.Equals(uint x, uint y)
            {
                return x == y;
            }

            int IEqualityComparer<uint>.GetHashCode(uint value)
            {
                return value.GetHashCode();
            }
        }

        /// <summary>
        /// This function gets rid of successive keys that reference the same node.
        /// This is a slight performance improvement for the LocateNode function.
        /// </summary>
        /// <returns></returns>
        public static uint[] CleanRepeatedNodes(List<uint> sortedKeys, Dictionary<uint, IMemcacheNode> keyToServer)
        {
            var length = sortedKeys.Count;
            if (length == 0)
                return new uint[0];

            var keyStack = new Stack<uint>(length);
            var previousKey = sortedKeys[0];
            var previousNode = keyToServer[previousKey];

            // Traverse the list of keys backwards
            for (int idx = length - 1; idx >= 0; idx--)
            {
                var currentKey = sortedKeys[idx];
                var currentNode = keyToServer[currentKey];
                if (currentNode != previousNode)
                {
                    keyStack.Push(currentKey);
                    previousNode = currentNode;
                }
            }

            // Treat the case where all keys point to the same node
            if (keyStack.Count == 0)
                keyStack.Push(sortedKeys[0]);

            return keyStack.ToArray();
        }
    }
}

#region License information

/* ************************************************************
 *
 *    Copyright (c) 2010 Attila Kiskó, enyim.com
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/

#endregion
