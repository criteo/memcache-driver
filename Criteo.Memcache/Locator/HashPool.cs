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
using System.Collections.Concurrent;
using System.Security.Cryptography;

namespace Criteo.Memcache.Locator
{
    internal class HashPool
    {
        private ConcurrentBag<HashAlgorithm> _hashAlgoPool;
        private Func<HashAlgorithm> _hashFactory;

        public HashPool(Func<HashAlgorithm> hashFactory)
        {
            _hashAlgoPool = new ConcurrentBag<HashAlgorithm>();
            _hashFactory = hashFactory;
        }

        public struct HashProxy : IDisposable
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

        public HashProxy Hash
        {
            get
            {
                return new HashProxy(_hashAlgoPool, _hashFactory);
            }
        }
    }
}
