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
using System.Threading.Tasks;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache
{
    /// <summary>
    /// This class is a simple proxy over the memcache client that gives the user a synchronous behavior
    /// </summary>
    public class SynchronousProxyClient
    {
        private MemcacheClient _client;
        private int _receiveTimeout;

        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="client">An already initialized client</param>
        /// <param name="receiveTimeout">The timeout for receive operation (Timeout.Inifite for blocking)</param>
        public SynchronousProxyClient(MemcacheClient client, int receiveTimeout)
        {
            _client = client;
            _receiveTimeout = receiveTimeout;
        }

        /// <summary>
        /// Retrieve the value for the given key if present
        /// </summary>
        /// <param name="key" />
        /// <returns>The fetched value, null if not present or any error was raised from memcached</returns>
        public byte[] Get(string key)
        {
            var taskSource = new TaskCompletionSource<byte[]>();

            if (!_client.Get(
                key: key,
                callback: (s, m) =>
                {
                    if (s == Status.NoError)
                        taskSource.SetResult(m);
                    else
                        taskSource.SetResult(null);
                }))
                taskSource.SetResult(null);

            if (taskSource.Task.Wait(_receiveTimeout))
                return taskSource.Task.Result;
            else
                return null;
        }

        /// <summary>
        /// Store the given value with the given key
        /// </summary>
        /// <param name="mode">See @StoreMode values</param>
        /// <param name="key" />
        /// <param name="value" />
        /// <param name="expire">Expiration of the value</param>
        /// <returns></returns>
        public bool Store(StoreMode mode, string key, byte[] value, TimeSpan expire)
        {
            var taskSource = new TaskCompletionSource<bool>();

            if (!_client.Store(
                mode: mode,
                key: key,
                message: value,
                expiration: expire,
                callback: s =>
                {
                    if (s == Status.NoError)
                        taskSource.SetResult(true);
                    else
                        taskSource.SetResult(false);
                }))
                taskSource.SetResult(false);

            if (taskSource.Task.Wait(_receiveTimeout))
                return taskSource.Task.Result;
            else
                return false;
        }

        /// <summary>
        /// Delete the providen key from the server
        /// </summary>
        /// <param name="key" />
        /// <returns>True when the deletion is effective</returns>
        public bool Delete(string key)
        {
            var taskSource = new TaskCompletionSource<bool>();

            if (!_client.Delete(
                key: key,
                callback: s =>
                {
                    if (s == Status.NoError)
                        taskSource.SetResult(true);
                    else
                        taskSource.SetResult(false);
                }))
                taskSource.SetResult(false);

            if (taskSource.Task.Wait(_receiveTimeout))
                return taskSource.Task.Result;
            else
                return false;
        }
    }
}
