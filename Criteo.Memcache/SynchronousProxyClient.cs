using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
