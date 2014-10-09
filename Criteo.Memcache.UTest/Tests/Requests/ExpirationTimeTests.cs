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
using Criteo.Memcache.Requests;
using NUnit.Framework;

namespace Criteo.Memcache.UTest.Tests.Requests
{
    [TestFixture]
    public class ExpirationTimeTests
    {
        private const int UNIX_TIME_20141305_045320PM = 1400000000;

        [Test]
        public void MemcachedTTLTest()
        {
            Assert.AreEqual(0, ExpirationTimeUtils.MemcachedTTL(ExpirationTimeUtils.Infinite), "Infinite TTL");

            var expire = TimeSpan.FromHours(48);
            Assert.AreEqual(expire.TotalSeconds, ExpirationTimeUtils.MemcachedTTL(expire), "A TimeSpan smaller than 30 days should be kept as is");

            expire = TimeSpan.FromDays(50);
            Assert.Less(UNIX_TIME_20141305_045320PM, ExpirationTimeUtils.MemcachedTTL(expire), "A TimeSpan greater than 30 days should be converted to a timestamp");
        }
    }
}