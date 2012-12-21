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
    public class MemcacheClient
    {
        private INodeLocator _locator;
        private IList<IMemcacheNode> _nodes;
        private MemcacheClientConfiguration _configuration;

        private NodeAllocator DefaultNodeFactory =
            (endPoint, configuration, SendRequest) => new MemcacheNode(endPoint, configuration, SendRequest);

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

        public MemcacheClient(MemcacheClientConfiguration configuration)
        {
            _configuration = configuration;
            _locator = configuration.NodeLocator ?? new RoundRobinNodeLocator();
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
                var node = (configuration.NodeFactory ?? DefaultNodeFactory)(nodeEndPoint, configuration, requeueRequest);
                _nodes.Add(node);
            }
        }

        protected bool SendRequest(IMemcacheRequest request)
        {
            var node = _locator.Locate(request.Key, _nodes);

            if (node == null)
            {
                if (_configuration.UnavaillablePolicy != Policy.Ignore)
                    throw new MemcacheException("No nodes are available");
                else
                    return false;
            }

            var res = node.TrySend(request, _configuration.EnqueueTimeout);
            if (!res && _configuration.QueueFullPolicy == Policy.Throw)
                throw new MemcacheException("Queue is full");

            return res;
        }

        public bool Set(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null)
        {
            return SendRequest(new SetRequest { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback });
        }

        public bool Update(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null)
        {
            return SendRequest(new UpdateRequest { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback });
        }

        public bool Add(string key, byte[] message, TimeSpan expiration, Action<Status> callback = null)
        {
            return SendRequest(new AddRequest { Key = key, Message = message, Expire = expiration, RequestId = NextRequestId, CallBack = callback });
        }

        public bool Get(string key, Action<Status, byte[]> callback)
        {
            return SendRequest(new GetRequest { Key = key, Callback = callback, RequestId = NextRequestId });
        }

        public Task<byte[]> Get(string key)
        {
            var taskSource = new TaskCompletionSource<byte[]>();

            if (!SendRequest(new GetRequest { Key = key, Callback = (s, m) =>  
                {
                    if (s == Status.NoError)
                        taskSource.SetResult(m);
                    else
                        taskSource.SetResult(null);
                }, RequestId = NextRequestId }))
                taskSource.SetResult(null);

            return taskSource.Task;
        }

        private int _currentRequestId = 0;
        protected uint NextRequestId
        {
            get
            {
                return (uint)Interlocked.Increment(ref _currentRequestId);
            }
        }
    }
}
