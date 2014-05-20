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
        private static DateTime Epock = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static TimeSpan Infinite = TimeSpan.FromMilliseconds(-1);    // = Timeout.InfiniteTimeSpan;  (On .NET Framework >= 4.5)

        /// <summary>
        /// Translate the request's exprire time to the memcached protocol format
        /// </summary>
        public static uint MemcachedTTL(TimeSpan Expire)
        {
            uint expire;

            if (Expire.CompareTo(TimeSpan.FromDays(30)) < 0)
                expire = (uint)Expire.TotalSeconds;
            else
                expire = (uint)(DateTime.UtcNow.Add(Expire) - Epock).TotalSeconds;

            return expire;
        }

        /// <summary>
        /// Check that the request's expire time is valid.
        /// Reject negative expiration times, except for the Infinite TimeSpan
        /// </summary>
        /// <param name="Expire"></param>
        /// <returns>True if the Expire time is valid</returns>
        public static bool IsValid(TimeSpan Expire)
        {
            return (Expire.Ticks >= 0) || (Expire.CompareTo(Infinite) == 0);
        }
    }
}