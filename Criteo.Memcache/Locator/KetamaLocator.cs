using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Security.Cryptography;

using Criteo.Memcache.Node;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Locator
{
    /// <summary>
    /// Basically very inspired (and copy/paste) from Enyim.Caching
    /// </summary>
    internal class KetamaLocator : INodeLocator
    {
        private const string DefaultHashName = "System.Security.Cryptography.MD5";
        private const int ServerAddressMutations = 160;

        #region Hash Pooling
        private ConcurrentBag<HashAlgorithm> _hashAlgoPool;
        private Func<HashAlgorithm> _hashFactory;

        protected struct HashProxy : IDisposable
        {
            private HashAlgorithm _value;
            private ConcurrentBag<HashAlgorithm> _pool;

            public HashProxy(ConcurrentBag<HashAlgorithm> pool, Func<HashAlgorithm> hashFactory)
            {
                _pool = pool;
                if (!pool.TryTake(out _value))
                    _value = hashFactory();
            }

            public HashAlgorithm Value
            {
                get
                {
                    return _value;
                }
            }

            public void Dispose()
            {
                if (_value != null)
                    _pool.Add(_value);
            }
        }

        protected HashProxy Hash
        {
            get
            {
                return new HashProxy(_hashAlgoPool, _hashFactory);
            }
        }
        #endregion Hash Pooling

        private IList<IMemcacheNode> _nodes;
        private LookupData _lookupData;
        //private Timer _resurrectTimer;
        private int _resurrectFreq;

        public KetamaLocator(string hashName = null, int resurectFreq = 1000)
        {
            _hashAlgoPool = new ConcurrentBag<HashAlgorithm>();
            _hashFactory = () => HashAlgorithm.Create(hashName = hashName ?? DefaultHashName);
            _resurrectFreq = resurectFreq;
        }

        public void Initialize(IList<IMemcacheNode> nodes)
        {
            _nodes = nodes;
            foreach (var node in nodes)
            {
                node.NodeDead += _ => Reinitialize();
                node.NodeAlive += _ => Reinitialize();
            }

            using(var hash = Hash)
                _lookupData = new LookupData(nodes, hash.Value);

            UpdateState(null);
            //_resurrectTimer = new Timer(UpdateState, null, _resurrectFreq, _resurrectFreq);
        }

        private void Reinitialize()
        {
            // filter only working nodes and 
            var newNodes = new List<IMemcacheNode>(_nodes.Count);
            foreach (var node in _nodes)
                if (!node.IsDead)
                    newNodes.Add(node);

            using (var hash = Hash)
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
            using (var hash = Hash)
                return hash.Value.ComputeHash(keyData).CopyToUIntNoRevert(0);
        }

        public IMemcacheNode Locate(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var ld = _lookupData;

            switch (ld.nodes.Length)
            {
                case 0: return null;
                case 1: var tmp = ld.nodes[0]; return tmp.IsDead ? null : tmp;
            }

            var retval = LocateNode(ld, this.GetKeyHash(key));

            return retval.IsDead ? null : retval;
        }

        private static IMemcacheNode LocateNode(LookupData ld, uint itemKeyHash)
        {
            // get the index of the server assigned to this hash
            int foundIndex = Array.BinarySearch<uint>(ld.keys, itemKeyHash);

            // no exact match
            if (foundIndex < 0)
            {
                // this is the nearest server in the list
                foundIndex = ~foundIndex;

                if (foundIndex == 0)
                {
                    // it's smaller than everything, so use the last server (with the highest key)
                    foundIndex = ld.keys.Length - 1;
                }
                else if (foundIndex >= ld.keys.Length)
                {
                    // the key was larger than all server keys, so return the first server
                    foundIndex = 0;
                }
            }

            if (foundIndex < 0 || foundIndex > ld.keys.Length)
                return null;
            
            return ld.keyToServer[ld.keys[foundIndex]];
        }

        /// <summary>
        /// this will encapsulate all the indexes we need for lookup
        /// so the instance can be reinitialized without locking
        /// </summary>
        private class LookupData
        {
            public IMemcacheNode[] nodes;
            // holds all server keys for mapping an item key to the server consistently
            public uint[] keys;
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

                this.keys = keys.ToArray();
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
    }
}
