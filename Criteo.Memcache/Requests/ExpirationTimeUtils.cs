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

namespace Criteo.Memcache.Requests
{
    internal static class ExpirationTimeUtils
    {
        private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static TimeSpan Infinite = TimeSpan.FromMilliseconds(-1);    // = Timeout.InfiniteTimeSpan;  (On .NET Framework >= 4.5)

        /// <summary>
        /// Translate the request's exprire time to the memcached protocol format
        /// </summary>
        public static uint MemcachedTTL(TimeSpan expire)
        {
            if (expire < TimeSpan.FromDays(30))
                return (uint)expire.TotalSeconds;

            return (uint)(DateTime.UtcNow.Add(expire) - Epoch).TotalSeconds;
        }

        /// <summary>
        /// Check that the request's expire time is valid.
        /// Reject negative expiration times, except for the Infinite TimeSpan
        /// </summary>
        /// <param name="expire" />
        /// <returns>True if the expire time is valid</returns>
        public static bool IsValid(TimeSpan expire)
        {
            return (expire.Ticks >= 0) || (expire.CompareTo(Infinite) == 0);
        }
    }
}