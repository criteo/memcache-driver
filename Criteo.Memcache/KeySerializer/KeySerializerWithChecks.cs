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
