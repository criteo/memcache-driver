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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

using Criteo.Memcache.Cluster;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Serializer;

namespace Criteo.Memcache
{
    public enum StoreMode
    {
        /// <summary>
        /// Creates or replace an already existing value
        /// </summary>
        Set,

        /// <summary>
        /// Fails with status KeyNotFound if not present
        /// </summary>
        Replace,

        /// <summary>
        /// Fails with status KeyExists if already present
        /// </summary>
        Add,
    }

    /// <summary>
    /// The callback policy for redundant requests (ADD/SET/ADD/REPLACE/DELETE).
    /// </summary>
    public enum CallBackPolicy
    {
        /// <summary>
        /// Call the callback with an OK status as soon as the first Status.NoError response is received, or with
        /// the last failed status if all responses are fails.
        /// </summary>
        AnyOK,

        /// <summary>
        /// Call the callback with an OK status if all responses are OK, or with the first received failed status
        /// </summary>
        AllOK,
    };

    /// <summary>
    /// The main class of the library
    /// </summary>
    public class MemcacheClient : IDisposable
    {
        private readonly MemcacheClientConfiguration _configuration;
        private readonly IMemcacheCluster _cluster;
        private bool _disposed = false;

        /// <summary>
        /// Raised when the server answer with a error code
        /// </summary>
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;

        private void OnMemcacheError(MemcacheResponseHeader header, IMemcacheRequest request)
        {
            if (MemcacheError != null)
                MemcacheError(header, request);
        }

        /// <summary>
        /// Raised when the transport layer fails
        /// </summary>
        public event Action<Exception> TransportError;

        private void OnTransportError(Exception e)
        {
            if (TransportError != null)
                TransportError(e);
        }

        /// <summary>
        /// Raised when a node seems unreachable
        /// </summary>
        public event Action<IMemcacheNode> NodeError;

        private void OnNodeError(IMemcacheNode node)
        {
            if (NodeError != null)
                NodeError(node);
        }

        /// <summary>
        /// Raised when a client callback thrown an exception
        /// It is the only to not let crash the IO Completion port thread and let the client know something went wrong
        /// </summary>
        public event Action<Exception> CallbackError;

        private void OnCallbackError(Exception e)
        {
            if (CallbackError != null)
                CallbackError(e);
        }

        /// <summary>
        /// Used to easily prevent unhandled exception from client callback
        /// </summary>
        /// <param name="callback"></param>
        /// <returns></returns>
        private Action<Status> SanitizeCallback(Action<Status> callback)
        {
            if (callback == null)
                return null;
            return v =>
            {
                try
                {
                    callback(v);
                }
                catch (Exception e)
                {
                    OnCallbackError(e);
                }
            };
        }

        /// <summary>
        /// Used to easily prevent unhandled exception from client callback or serializer
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="callback"></param>
        /// <param name="serializer"></param>
        /// <returns></returns>
        private Action<Status, byte[]> SanitizeCallback<T>(Action<Status, T> callback, ISerializer<T> serializer)
        {
            if (callback == null)
                return null;
            return (Status s, byte[] m) =>
            {
                try
                {
                    T value = serializer.FromBytes(m);
                    callback(s, value);
                }
                catch (Exception e)
                {
                    OnCallbackError(e);
                }
            };
        }

        private void RegisterEvents(IMemcacheNode node)
        {
            node.MemcacheError += OnMemcacheError;
            node.TransportError += OnTransportError;
            node.NodeDead += OnNodeError;
        }

        private void UnregisterEvents(IMemcacheNode node)
        {
            node.MemcacheError -= OnMemcacheError;
            node.TransportError -= OnTransportError;
            node.NodeDead -= OnNodeError;
        }

        /// <summary>
        /// The constructor, see @MemcacheClientConfiguration for details
        /// </summary>
        /// <param name="configuration"></param>
        public MemcacheClient(MemcacheClientConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentException("Client config should not be null");

            _configuration = configuration;
            _cluster = (configuration.ClusterFactory ?? MemcacheClientConfiguration.DefaultClusterFactory)(configuration);
            _cluster.NodeAdded += RegisterEvents;
            _cluster.NodeRemoved += UnregisterEvents;
            _cluster.OnError += OnCallbackError;
            _cluster.Initialize();
        }

        /// <summary>
        /// Sends a request with the policy defined with the configuration object, to multiple nodes if the replicas setting
        /// is different from zero.
        /// </summary>
        /// <param name="request">A memcache request derived from RedundantRequest</param>
        /// <returns>
        /// True if the request was sent to at least one node. The caller will receive a callback (if not null).
        /// False if the request could not be sent to any node. In that case, the callback will not be called.
        /// </returns>
        protected bool SendRequest(IRedundantRequest request)
        {
            int countTrySends = 0;
            int countTrySendsOK = 0;

            foreach (var node in _cluster.Locator.Locate(request))
            {
                countTrySends++;
                if (!node.IsDead && node.TrySend(request, _configuration.QueueTimeout))
                    countTrySendsOK++;

                // Break after trying to send the request to replicas+1 nodes
                if (countTrySends > request.Replicas)
                    break;
            }

            // The callback will not be called
            if (countTrySendsOK == 0)
                return false;

            // If the request was sent to less than Replicas+1 nodes, fail the remaining ones.
            for (; countTrySendsOK <= request.Replicas; countTrySendsOK++)
                request.Fail();

            return true;
        }

        /// <summary>
        /// Sends a request (no client replication handled)
        /// If the master node in dead, try to sends a fallback request to the failover node
        /// sending the failover request
        /// </summary>
        /// <param name="request">A memcache request</param>
        /// <param name="requestReplica">The failover request to send to a replica</param>
        /// <returns>
        /// True if the request was sent to at least one node. The caller will receive a callback (if not null).
        /// False if the request could not be sent to any node. In that case, the callback will not be called.
        /// </returns>
        protected bool SendRequestWithReplica(ICouchbaseRequest request, ICouchbaseRequest requestReplica)
        {
            var req = request;
            foreach (var node in _cluster.Locator.Locate(request))
            {
                if (!node.IsDead && node.TrySend(req, _configuration.QueueTimeout))
                    return true;

                req = requestReplica;
                requestReplica.VBucket = request.VBucket;
            }

            return false;
        }

        /// <summary>
        /// Sets the key with the given message with the given TTL
        /// </summary>
        /// <param name="key" />
        /// <param name="message" />
        /// <param name="expiration" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <param name="callbackPolicy" />
        /// <returns></returns>
        public bool Set<T>(string key, T message, TimeSpan expiration, Action<Status> callback = null, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            return Store<T>(StoreMode.Set, key, message, expiration, callback, callbackPolicy);
        }

        /// <summary>
        /// Sets the key with the given message with the given TTL
        /// Fails if the key doesn't exists (KeyNotFound as status on callback)
        /// </summary>
        /// <param name="key" />
        /// <param name="message" />
        /// <param name="expiration" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <param name="callbackPolicy" />
        /// <returns></returns>
        public bool Update<T>(string key, T message, TimeSpan expiration, Action<Status> callback = null, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            return Store(StoreMode.Replace, key, message, expiration, callback, callbackPolicy);
        }

        /// <summary>
        /// Sets the key with the given message with the given TTL
        /// Fails if the key already exists (KeyExists as status on callback)
        /// </summary>
        /// <param name="key" />
        /// <param name="message" />
        /// <param name="expiration" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <param name="callbackPolicy" />
        /// <returns></returns>
        public bool Add<T>(string key, T message, TimeSpan expiration, Action<Status> callback = null, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            return Store(StoreMode.Add, key, message, expiration, callback, callbackPolicy);
        }

        public bool Store<T>(StoreMode mode, string key, T message, TimeSpan expiration, Action<Status> callback = null, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            Opcode op;
            if (mode == StoreMode.Add)
                op = Opcode.Add;
            else if (mode == StoreMode.Replace)
                op = Opcode.Replace;
            else if (mode == StoreMode.Set)
                op = Opcode.Set;
            else
                throw new ArgumentException("Unsupported operation " + mode);

            var keyAsBytes = _configuration.KeySerializer.SerializeToBytes(key);
            var serializer = _configuration.SerializerOf<T>();
            var request = new SetRequest
            {
                Key = keyAsBytes,
                Message = serializer.ToBytes(message),
                Flags = serializer.TypeFlag,
                Expire = expiration,
                RequestId = NextRequestId,
                CallBack = SanitizeCallback(callback),
                CallBackPolicy = callbackPolicy,
                Replicas = _configuration.Replicas,
                RequestOpcode = op,
            };

            return SendRequest(request);
        }

        /// <summary>
        /// This command will add the specified amount to the requested counter.
        /// </summary>
        /// <remarks>
        /// If you want to set the value of the counter with add/set/replace, the objects data must be the ascii representation of the value and not the byte values of a 64 bit integer.
        /// </remarks>
        /// <param name="key" />
        /// <param name="delta" />
        /// <param name="initial"/>
        /// <param name="expiration" />
        /// <param name="callback" />
        /// <returns>False if synchronously failed</returns>
        public bool Increment(string key, ulong delta, ulong initial, TimeSpan expiration, Action<Status, ulong> callback)
        {
            return IncrementInternal(key, delta, initial, expiration, callback, Opcode.Increment);
        }

        /// <summary>
        /// This command will remove the specified amount to the requested counter.
        /// </summary>
        /// <remarks>
        /// If you want to set the value of the counter with add/set/replace, the objects data must be the ascii representation of the value and not the byte values of a 64 bit integer.
        /// Decrementing a counter will never result in a "negative value" (or cause the counter to "wrap"). instead the counter is set to 0. Incrementing the counter may cause the counter to wrap.
        /// </remarks>
        /// <param name="key" />
        /// <param name="delta" />
        /// <param name="initial"/>
        /// <param name="expiration" />
        /// <param name="callback" />
        /// <returns>False if synchronously failed</returns>
        public bool Decrement(string key, ulong delta, ulong initial, TimeSpan expiration, Action<Status, ulong> callback)
        {
            return IncrementInternal(key, delta, initial, expiration, callback, Opcode.Decrement);
        }

        internal bool IncrementInternal(string key, ulong increment, ulong seed, TimeSpan expiration, Action<Status, ulong> callback, Opcode code)
        {
            var keyAsBytes = _configuration.KeySerializer.SerializeToBytes(key);
            var serializer = _configuration.SerializerOf<ulong>();
            var request = new IncrementRequest
            {
                Key = keyAsBytes,
                CallBack = SanitizeCallback(callback, serializer),
                Expire = expiration,
                RequestId = NextRequestId,
                Replicas = _configuration.Replicas,
                RequestOpcode = code,
                Delta = increment,
                Initial = seed,
            };

            return SendRequest(request);
        }

        /// <summary>
        /// Fetch the value for the given key
        /// </summary>
        /// <param name="key" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <param name="callbackPolicy" />
        /// <returns></returns>
        public bool Get<T>(string key, Action<Status, T> callback, CallBackPolicy callbackPolicy = CallBackPolicy.AnyOK)
        {
            var keyAsBytes = _configuration.KeySerializer.SerializeToBytes(key);
            var serializer = _configuration.SerializerOf<T>();
            var request = new GetRequest
            {
                Key = keyAsBytes,
                CallBack = SanitizeCallback(callback, serializer),
                CallBackPolicy = callbackPolicy,
                RequestId = NextRequestId,
                Replicas = _configuration.Replicas,
            };

            return SendRequest(request);
        }

        /// <summary>
        /// Fetch the value for the given key, fallback with replica read if the master is not rechable
        /// Only usable with couchbase, not with a memcached cluster
        /// </summary>
        /// <param name="key" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <returns></returns>
        public bool GetWithReplica<T>(string key, Action<Status, T> callback)
        {
            var keyAsBytes = _configuration.KeySerializer.SerializeToBytes(key);
            var serializer = _configuration.SerializerOf<T>();
            var request = new GetRequest
            {
                Key = keyAsBytes,
                CallBack = SanitizeCallback(callback, serializer),
                RequestId = NextRequestId,
                RequestOpcode = Opcode.Get,
            };

            var replicaRequest = new GetRequest
            {
                Key = keyAsBytes,
                CallBack = SanitizeCallback(callback, serializer),
                RequestId = NextRequestId,
                RequestOpcode = Opcode.ReplicaRead,
            };

            return SendRequestWithReplica(request, replicaRequest);
        }

        /// <summary>
        /// Fetch the value for the given key
        /// and prolongs it to the given TTL
        /// </summary>
        /// <param name="key" />
        /// <param name="expire" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <param name="callbackPolicy" />
        /// <returns></returns>
        public bool GetAndTouch<T>(string key, TimeSpan expire, Action<Status, T> callback, CallBackPolicy callbackPolicy = CallBackPolicy.AnyOK)
        {
            var keyAsBytes = _configuration.KeySerializer.SerializeToBytes(key);
            var serializer = _configuration.SerializerOf<T>();
            var request = new GetRequest
            {
                RequestOpcode = Opcode.GAT,
                Expire = expire,
                Key = keyAsBytes,
                CallBack = SanitizeCallback(callback, serializer),
                CallBackPolicy = callbackPolicy,
                RequestId = NextRequestId,
                Replicas = _configuration.Replicas
            };

            return SendRequest(request);
        }

        /// <summary>
        /// Delete the entry associated with the given key
        /// </summary>
        /// <param name="key" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <param name="callbackPolicy" />
        /// <returns></returns>
        public bool Delete(string key, Action<Status> callback, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            var keyAsBytes = _configuration.KeySerializer.SerializeToBytes(key);
            var request = new DeleteRequest
            {
                Key = keyAsBytes,
                CallBack = SanitizeCallback(callback),
                CallBackPolicy = callbackPolicy,
                RequestId = NextRequestId,
                Replicas = _configuration.Replicas
            };

            return SendRequest(request);
        }

        public void Ping(Action<EndPoint, Status> callback)
        {
            foreach (var node in _cluster.Nodes)
            {
                if (node.IsDead)
                    callback(node.EndPoint, Status.InternalError);
                else
                {
                    var localNode = node;
                    var request = new NoOpRequest
                    {
                        Callback = r => callback(localNode.EndPoint, r.Status),
                        RequestId = NextRequestId
                    };

                    if (!node.TrySend(request, _configuration.QueueTimeout))
                        callback(node.EndPoint, Status.InternalError);
                }
            }
        }

        /// <summary>
        /// Warmup connection to each node
        /// </summary>
        /// <param name="callback">called when all nodes have been contacted</param>
        public void Warmup(Action callback)
        {
            var aliveNodes = _cluster.Nodes.Where(n => !n.IsDead).ToList();
            var answerToGet = aliveNodes.Count;

            Action<MemcacheResponseHeader> onResponse = r =>
                    {
                        if (Interlocked.Decrement(ref answerToGet) == 0)
                        {
                            callback();
                        }
                    };

            foreach (var node in aliveNodes)
            {
                var request = new NoOpRequest
                {
                    Callback = onResponse,
                    RequestId = NextRequestId
                };

                if (!node.TrySend(request, _configuration.QueueTimeout))
                {
                    onResponse(default(MemcacheResponseHeader));
                }
            }
        }

        /// <summary>
        /// Retrieve stats from all alive nodes
        /// </summary>
        /// <param name="key" />
        /// <param name="callback" />
        public void Stats(string key, Action<EndPoint, IDictionary<string, string>> callback)
        {
            var keyAsBytes = _configuration.KeySerializer.SerializeToBytes(key);

            foreach (var node in _cluster.Nodes)
            {
                if (node.IsDead)
                {
                    callback(node.EndPoint, null);
                }
                else
                {
                    var localNode = node;
                    var request = new StatRequest
                    {
                        Key = keyAsBytes,
                        Callback = r => callback(localNode.EndPoint, r),
                        RequestId = NextRequestId
                    };

                    if (!node.TrySend(request, _configuration.QueueTimeout))
                        callback(node.EndPoint, null);
                }
            }
        }

        /// <summary>
        /// Attempt to shutdown the memcache client.
        /// This method may return false if some requests are still pending, allowing for a graceful shutdown.
        /// After a call to Shutdown, the client will reject any new request.
        /// </summary>
        /// <param name="force">Force an immediate shutdown and fail all pending requests.</param>
        /// <returns>True if the client was successfully shut down.</returns>
        public bool Shutdown(bool force = false)
        {
            // Shutdown all nodes, don't stop on the first one returning false!
            // Do not reverse '.Shutdown' and 'done'.
            return _cluster.Nodes.Aggregate(true, (done, node) => node.Shutdown(force) && done);
        }

        /// <summary>
        /// Number of alive nodes
        /// </summary>
        public int AliveNodes
        {
            get
            {
                return _cluster.Nodes.Count(node => !node.IsDead);
            }
        }

        private int _currentRequestId = 0;

        protected uint NextRequestId
        {
            get
            {
                return (uint)Interlocked.Increment(ref _currentRequestId);
            }
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
                _cluster.Dispose();

            _disposed = true;
        }

        #endregion IDisposable
    }
}
