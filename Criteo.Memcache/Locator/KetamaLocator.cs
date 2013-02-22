using System;
using System.Collections.Generic;
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

        [ThreadStatic]
        private HashAlgorithm _hashAlgo;
        protected HashAlgorithm Hash 
        { 
            get
            {
                if (_hashAlgo == null)
                    _hashAlgo = _hashFactory();
                return _hashAlgo;
            }
        }
        private Func<HashAlgorithm> _hashFactory;
        private LookupData _lookupData;

        public KetamaLocator(string hashName = null)
        {
            _hashFactory = () => HashAlgorithm.Create(hashName = hashName ?? DefaultHashName);
        }

        public void Initialize(IList<IMemcacheNode> nodes)
        {
            int PartCount = Hash.HashSize / (8 * sizeof(int)); // HashSize is in bits, uint is 4 bytes long
            if (PartCount < 1) throw new ArgumentOutOfRangeException("The hash algorithm must provide at least 32 bits long hashes");

            var keys = new List<uint>(nodes.Count);
            var keyToServer = new Dictionary<uint, IMemcacheNode>(nodes.Count, new UIntEqualityComparer());

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
                    byte[] data = Hash.ComputeHash(Encoding.ASCII.GetBytes(address + "-" + mutation));

                    for (int p = 0; p < PartCount; p++)
                    {
                        var key = data.CopyToUInt(p * 4);
                        keys.Add(key);
                        keyToServer[key] = currentNode;
                    }
                }
            }
            keys.Sort();

            var lookupData = new LookupData
            {
                keys = keys.ToArray(),
                keyToServer = keyToServer,
                nodes = nodes.ToArray(),
            };

            Interlocked.Exchange(ref _lookupData, lookupData);
        }

        private uint GetKeyHash(string key)
        {
            var keyData = Encoding.UTF8.GetBytes(key);
            return Hash.ComputeHash(keyData).CopyToUInt(0);
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

            // if the result is not alive then try to mutate the item key and 
            // find another node this way we do not have to reinitialize every 
            // time a node dies/comes back
            // (DefaultServerPool will resurrect the nodes in the background without affecting the hashring)
            if (retval.IsDead)
            {
                for (var i = 0; i < ld.nodes.Length; i++)
                {
                    // -- this is from spymemcached so we select the same node for the same items
                    ulong tmpKey = (ulong)GetKeyHash(i + key);
                    tmpKey += (uint)(tmpKey ^ (tmpKey >> 32));
                    tmpKey &= 0xffffffffL; /* truncate to 32-bits */
                    // -- end

                    retval = LocateNode(ld, (uint)tmpKey);

                    if (!retval.IsDead) return retval;
                }
            }

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
