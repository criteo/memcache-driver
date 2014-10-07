using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.Memcache.KeySerializer
{
    /// <summary>
    /// Generic key value serializer which checks for maximum length.
    /// </summary>
    /// <typeparam name="KeyType">Type of the Key value to be serialized</typeparam>
    public abstract class KeySerializerWithChecks<KeyType> : IKeySerializer<KeyType>
    {
        public int MaxKeyLength { get; set; }

        /// <summary>
        /// Empty constructor. Sets MaxKeyLength to 250 (memcached implementation).
        /// </summary>
        public KeySerializerWithChecks()
        {
            MaxKeyLength = 250;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="maxKeyLength">Maximum key length</param>
        public KeySerializerWithChecks(int maxKeyLength)
        {
            MaxKeyLength = maxKeyLength;
        }

        public byte[] SerializeToBytes(KeyType value)
        {
            var bytes = DoSerializeToBytes(value);
            if (bytes != null && bytes.Length > MaxKeyLength)
                throw new ArgumentException("Serialized key is longer than allowed (length: " + bytes.Length + ", max: " + MaxKeyLength + ")");

            return bytes;
        }

        protected abstract byte[] DoSerializeToBytes(KeyType value);
    }
}
