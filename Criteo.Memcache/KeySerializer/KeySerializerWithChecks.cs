using System;

namespace Criteo.Memcache.KeySerializer
{
    /// <summary>
    /// Generic key value serializer which checks for maximum length.
    /// </summary>
    /// <typeparam name="TKeyType">Type of the Key value to be serialized</typeparam>
    public abstract class KeySerializerWithChecks<TKeyType> : IKeySerializer<TKeyType>
    {
        public int MaxKeyLength { get; set; }

        /// <summary>
        /// Empty constructor. Sets MaxKeyLength to 250 (memcached implementation).
        /// </summary>
        protected KeySerializerWithChecks()
        {
            MaxKeyLength = 250;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="maxKeyLength">Maximum key length</param>
        protected KeySerializerWithChecks(int maxKeyLength)
        {
            MaxKeyLength = maxKeyLength;
        }

        public byte[] SerializeToBytes(TKeyType value)
        {
            var bytes = DoSerializeToBytes(value);
            if (bytes != null && bytes.Length > MaxKeyLength)
                throw new ArgumentException("Serialized key is longer than allowed (length: " + bytes.Length + ", max: " + MaxKeyLength + ")");

            return bytes;
        }

        protected abstract byte[] DoSerializeToBytes(TKeyType value);
    }
}
