using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Node;
using Criteo.Memcache.Locator;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;

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

            Action<IMemcacheRequest> requeueRequest;
            switch(configuration.NodeDeadPolicy)
            {
                case RequeuePolicy.Requeue:
                    requeueRequest = req => SendRequest(req);
                    break;
                default:
                    requeueRequest = _ => { };
                    break;
            }

            foreach (var nodeEndPoint in configuration.NodesEndPoints)
            {
                var node = (configuration.NodeFactory ?? MemcacheClientConfiguration.DefaultNodeFactory)(nodeEndPoint, configuration);
                _nodes.Add(node);
            }

            _locator.Initialize(_nodes);
        }

        /// <summary>
        /// Sends a request with the policy defined with the configuration object
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        protected bool SendRequest(IMemcacheRequest request)
        {
            var node = _locator.Locate(request.Key);

            if (node == null)
            {
                if (_configuration.UnavaillablePolicy != Policy.Ignore)
                    throw new MemcacheException("No nodes are available");
                else
                    return false;
            }

            var res = node.TrySend(request, _configuration.QueueTimeout);
            if (!res && _configuration.QueueFullPolicy == Policy.Throw)
                throw new MemcacheException("Queue is full");

            return res;
        }

        /// <summary>
        /// Sets the key with the given message with the given TTL
        /// </summary>
        /// <param name="key" />
        /// <param name="message" />
        /// <param name="expiration" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <returns></returns>
        public bool Set(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null)
        {
            return Store(StoreMode.Set, key, message, expiration, callback);
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
        public bool Update(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null)
        {
            return Store(StoreMode.Replace, key, message, expiration, callback);
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
        public bool Add(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null)
        {
            return Store(StoreMode.Add, key, message, expiration, callback);
        }

        public bool Store(StoreMode mode, string key, byte[] message, TimeSpan expiration, Action<Status> callback = null)
        {
            switch (mode)
            {
                case StoreMode.Set:
                    return SendRequest(new SetRequest { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback });
                case StoreMode.Replace:
                    return SendRequest(new SetRequest { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback, Code = Opcode.Replace});
                case StoreMode.Add:
                    return SendRequest(new SetRequest { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback, Code = Opcode.Add });
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
        public bool Get(string key, Action<Status, byte[]> callback)
        {
            return SendRequest(new GetRequest { Key = key, CallBack = callback, RequestId = NextRequestId });
        }

        /// <summary>
        /// Delete the entry associated with the given key
        /// </summary>
        /// <param name="key" />
        /// <param name="callback">Will be called after the memcached respond</param>
        /// <returns></returns>
        public bool Delete(string key, Action<Status> callback)
        {
            return SendRequest(new DeleteRequest { Key = key, CallBack = callback, RequestId = NextRequestId });
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
