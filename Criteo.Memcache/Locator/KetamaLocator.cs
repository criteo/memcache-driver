using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;

namespace Criteo.Memcache.Locator
{
    /// <summary>
    /// Basically very inspired (and copy/paste) from Enyim.Caching
    /// </summary>
    internal class KetamaLocator : INodeLocator
    {
        private const string DefaultHashName = "System.Security.Cryptography.MD5";
        private const int ServerAddressMutations = 160;

        private HashPool _hashPool;
        private IList<IMemcacheNode> _nodes;
        private LookupData _lookupData;

        public KetamaLocator(string hashName = null)
        {
            _hashPool = new HashPool(() => HashAlgorithm.Create(hashName ?? DefaultHashName));
        }

        public void Initialize(IList<IMemcacheNode> nodes)
        {
            _nodes = nodes;
            foreach (var node in nodes)
            {
                node.NodeDead += _ => Reinitialize();
                node.NodeAlive += _ => Reinitialize();
            }

            using (var hash = _hashPool.Hash)
                _lookupData = new LookupData(nodes, hash.Value);

            UpdateState(null);
        }

        private void Reinitialize()
        {
            // filter only working nodes and
            var newNodes = new List<IMemcacheNode>(_nodes.Count);
            foreach (var node in _nodes)
                if (!node.IsDead)
                    newNodes.Add(node);

            using (var hash = _hashPool.Hash)
                Interlocked.Exchange(ref _lookupData, new LookupData(newNodes, hash.Value));
        }

        /// <summary>
        /// Checks if the liveness of the nodes has changed
        /// </summary>
        private void UpdateState(object _)
        {
            var ld = _lookupData;

            var nodeSet = new HashSet<IMemcacheNode>(ld.nodes);
            bool mustRecompute = false;
            foreach (var node in _nodes)
            {
                // this node is a new dead, recompte all
                if (node.IsDead && nodeSet.Contains(node))
                {
                    mustRecompute = true;
                    break;
                }
                // this node has changed to alive, recompute all
                if (!node.IsDead && !nodeSet.Contains(node))
                {
                    mustRecompute = true;
                    break;
                }
            }

            if(mustRecompute)
                Reinitialize();
        }

        private uint GetKeyHash(string key)
        {
            var keyData = Encoding.UTF8.GetBytes(key);
            using (var hash = _hashPool.Hash)
                return hash.Value.ComputeHash(keyData).CopyToUIntNoRevert(0);
        }

        public IEnumerable<IMemcacheNode> Locate(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var ld = _lookupData;

            switch (ld.nodes.Length)
            {
                case 0:
                    yield break;
                case 1:
                    var firstNode = ld.nodes[0];
                    if(!firstNode.IsDead)
                        yield return firstNode;
                    yield break;
            }

            // Return alive nodes only.
            foreach(var retNode in LocateNode(ld, this.GetKeyHash(key)))
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
            int foundIndex = Array.BinarySearch<uint>(ld.sortedKeys, itemKeyHash);
            IMemcacheNode node1 = null;
            IMemcacheNode node2 = null;
            HashSet<IMemcacheNode> usedNodes = null;

            // no exact match
            if (foundIndex < 0)
            {
                // this is the next greater index in the list
                foundIndex = ~foundIndex;

                if (foundIndex >= ld.sortedKeys.Length)
                {
                    // the key was larger than all server keys, so return the first server
                    foundIndex = 0;
                }
            }

            // Paranoid check
            if (foundIndex < 0 || foundIndex >= ld.sortedKeys.Length)
                yield break;

            // Return distinct nodes. Exit after a complete loop over the keys.
            int startingIndex = foundIndex;
            do
            {
                IMemcacheNode node = ld.keyToServer[ld.sortedKeys[foundIndex]];
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
                        usedNodes = new HashSet<IMemcacheNode>() { node1, node2 };
                    }
                    if (!usedNodes.Contains(node))
                    {
                        usedNodes.Add(node);
                        yield return node;
                    }
                }

                foundIndex++;
                if (foundIndex >= ld.sortedKeys.Length)
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
            // holds all server nodes. Order and size of this array is not consistent with array sortedKeys.
            public IMemcacheNode[] nodes;
            // holds all server keys for mapping an item key to the server consistently
            public uint[] sortedKeys;
            // used to lookup a server based on its key
            public Dictionary<uint, IMemcacheNode> keyToServer;

            public LookupData(IList<IMemcacheNode> nodes, HashAlgorithm hash)
            {
                int PartCount = hash.HashSize / (8 * sizeof(uint)); // HashSize is in bits, uint is 4 bytes long
                if (PartCount < 1) throw new ArgumentOutOfRangeException("The hash algorithm must provide at least 32 bits long hashes");

                var keys = new List<uint>(nodes.Count);
                keyToServer = new Dictionary<uint, IMemcacheNode>(nodes.Count, new UIntEqualityComparer());

                for (int nodeIndex = 0; nodeIndex < nodes.Count; nodeIndex++)
                {
                    var currentNode = nodes[nodeIndex];

                    // every server is registered numberOfKeys times
                    // using UInt32s generated from the different parts of the hash
                    // i.e. hash is 64 bit:
                    // 01 02 03 04 05 06 07
                    // server will be stored with keys 0x07060504 & 0x03020100
                    string address = currentNode.EndPoint.ToString();

                    for (int mutation = 0; mutation < ServerAddressMutations / PartCount; mutation++)
                    {
                        byte[] data = hash.ComputeHash(Encoding.ASCII.GetBytes(address + "-" + mutation));

                        for (int p = 0; p < PartCount; p++)
                        {
                            var key = data.CopyToUIntNoRevert(p * 4);
                            keys.Add(key);
                            keyToServer[key] = currentNode;
                        }
                    }
                }
                keys.Sort();

                this.sortedKeys = CleanRepeatedNodes(keys, keyToServer);
                this.nodes = nodes.ToArray();
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
            int Length = sortedKeys.Count;

            if (Length == 0)
            {
                return new uint[0];
            }

            Stack<uint> keyStack = new Stack<uint>(Length);
            uint previousKey = sortedKeys[0];
            IMemcacheNode previousNode = keyToServer[previousKey];

            // Traverse the list of keys backwards
            for (int idx = Length - 1; idx >= 0; idx--)
            {
                uint currentKey = sortedKeys[idx];
                IMemcacheNode currentNode = keyToServer[currentKey];
                if (currentNode != previousNode)
                {
                    keyStack.Push(currentKey);
                    previousNode = currentNode;
                }
                previousKey = currentKey;
            }

            // Treat the case where all keys point to the same node
            if (keyStack.Count == 0)
            {
                keyStack.Push(sortedKeys[0]);
            }

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
