using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Node;
using Criteo.Memcache.Locator;
using Criteo.Memcache.Headers;

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
    /// The policy for redundant requests (ADD/SET/ADD/REPLACE/DELETE) can be:
    /// AnyOK: Call the callback with an OK status as soon as the first Status.NoError response is received, or with
    ///        the last failed status if all responses are fails.
    /// AllOK: Call the callback with an OK status if all responses are OK, or with the first received failed status 
    /// </summary>
    public enum CallBackPolicy
    {
        AnyOK,
        AllOK,
    };

    /// <summary>
    /// The main class of the library
    /// </summary>
    public class MemcacheClient : IDisposable
    {
        private INodeLocator _locator;
        private IList<IMemcacheNode> _nodes;
        private MemcacheClientConfiguration _configuration;


        /// <summary>
        /// Raised when the server answer with a error code
        /// </summary>
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError
        {
            add
            {
                foreach (var node in _nodes)
                    node.MemcacheError += value;
            }
            remove
            {
                foreach (var node in _nodes)
                    node.MemcacheError -= value;
            }
        }

        /// <summary>
        /// Raised when the transport layer fails
        /// </summary>
        public event Action<Exception> TransportError
        {
            add
            {
                foreach (var node in _nodes)
                    node.TransportError += value;
            }
            remove
            {
                foreach (var node in _nodes)
                    node.TransportError -= value;
            }
        }

        /// <summary>
        /// Raised when a node seems unreachable
        /// </summary>
        public event Action<IMemcacheNode> NodeError
        {
            add
            {
                foreach (var node in _nodes)
                    node.NodeDead += value;
            }
            remove
            {
                foreach (var node in _nodes)
                    node.NodeDead -= value;
            }
        }

        /// <summary>
        /// The constructor, see @MemcacheClientConfiguration for details
        /// </summary>
        /// <param name="configuration"></param>
        public MemcacheClient(MemcacheClientConfiguration configuration)
        {
            _configuration = configuration;
            _locator = configuration.NodeLocator ?? MemcacheClientConfiguration.DefaultLocatorFactory();
            _nodes = new List<IMemcacheNode>(configuration.NodesEndPoints.Count);

            foreach (var nodeEndPoint in configuration.NodesEndPoints)
            {
                var node = (configuration.NodeFactory ?? MemcacheClientConfiguration.DefaultNodeFactory)(nodeEndPoint, configuration);
                _nodes.Add(node);
            }

            _locator.Initialize(_nodes);
        }

        /// <summary>
        /// Sends a request with the policy defined with the configuration object, to multiple nodes if necessary
        /// according to the redundancy setting in the request.
        /// </summary>
        /// <param name="request">A memcache request derived from RedundantRequest</param>
        /// <returns>False if the request could not be sent to any node. In that case, the callback will not be called.</returns>
        protected bool SendRequest(IMemcacheRequest request)
        {
            int countSentRequests = 0;
            bool returnStatus = false;

            foreach (var node in _locator.Locate(request.Key))
            {
                if (node.TrySend(request, _configuration.QueueTimeout))
                {
                    returnStatus = true;
                    countSentRequests++;
                    if (countSentRequests > request.Replicas)
                    {
                        break;
                    }
                }
            }

            request.Sent(countSentRequests);

            return returnStatus;
        }

        /// <summary>
        /// Sets the key with the given message with the given TTL
        /// </summary>
        /// <param name="key" />
        /// <param name="message" />
        /// <param name="expiration" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <returns></returns>
        public bool Set(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            return Store(StoreMode.Set, key, message, expiration, callback, callbackPolicy);
        }

        /// <summary>
        /// Sets the key with the given message with the given TTL
        /// Fails if the key doesn't exists (KeyNotFound as status on callback)
        /// </summary>
        /// <param name="key" />
        /// <param name="message" />
        /// <param name="expiration" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <returns></returns>
        public bool Update(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
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
        /// <returns></returns>
        public bool Add(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            return Store(StoreMode.Add, key, message, expiration, callback, callbackPolicy);
        }

        public bool Store(StoreMode mode, string key, byte[] message, TimeSpan expiration, Action<Status> callback = null, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            switch (mode)
            {
                case StoreMode.Set:
                    return SendRequest(new SetRequest(_configuration.Replicas) { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback, CallBackPolicy = callbackPolicy });
                case StoreMode.Replace:
                    return SendRequest(new SetRequest(_configuration.Replicas) { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback, CallBackPolicy = callbackPolicy, Code = Opcode.Replace });
                case StoreMode.Add:
                    return SendRequest(new SetRequest(_configuration.Replicas) { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback, CallBackPolicy = callbackPolicy, Code = Opcode.Add });
                default:
                    return false;
            }
        }

        /// <summary>
        /// Fetch the value for the given key
        /// </summary>
        /// <param name="key" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <returns></returns>
        public bool Get(string key, Action<Status, byte[]> callback, CallBackPolicy callbackPolicy = CallBackPolicy.AnyOK)
        {
            return SendRequest(new GetRequest(_configuration.Replicas) { Key = key, CallBack = callback, CallBackPolicy = callbackPolicy, RequestId = NextRequestId });
        }

        /// <summary>
        /// Delete the entry associated with the given key
        /// </summary>
        /// <param name="key" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <returns></returns>
        public bool Delete(string key, Action<Status> callback, CallBackPolicy callbackPolicy = CallBackPolicy.AllOK)
        {
            return SendRequest(new DeleteRequest(_configuration.Replicas) { Key = key, CallBack = callback, CallBackPolicy = callbackPolicy, RequestId = NextRequestId });
        }

        /// <summary>
        /// Retrieve stats from all alive nodes
        /// </summary>
        /// <param name="callback"></param>
        public void Stats(string key, Action<EndPoint, IDictionary<string, string>> callback)
        {
            foreach (var node in _nodes)
            {
                if (node.IsDead)
                    callback(node.EndPoint, null);
                else
                {
                    var localNode = node;
                    if (!node.TrySend(new StatRequest { Key = key, Callback = r => callback(localNode.EndPoint, r), RequestId = NextRequestId }, _configuration.QueueTimeout))
                        callback(node.EndPoint, null);
                }
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

        public void Dispose()
        {
            foreach (var node in _nodes)
                node.Dispose();
        }
    }
}
