using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Criteo.MemcacheClient.Configuration;
using Criteo.MemcacheClient.Requests;
using Criteo.MemcacheClient.Node;

namespace Criteo.MemcacheClient
{
    public class MemcacheClient
    {
        private INodeLocator _locator;
        private IList<IMemcacheNode> _nodes;
        private MemcacheClientConfiguration _configuration;

        private NodeAllocator DefaultNodeFactory =
            (endPoint, configuration) => new MemcacheNode(endPoint, configuration);

        public event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheError
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

        public event Action<MemcacheNode> NodeError
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
            foreach (var nodeEndPoint in configuration.NodesEndPoints)
            {
                var node = (configuration.NodeFactory ?? DefaultNodeFactory)(nodeEndPoint, configuration);
                node.NodeFlush += RequeueNodeRequests;
                _nodes.Add(node);
            }
        }

        private void RequeueNodeRequests(MemcacheNode node)
        {
            IMemcacheRequest request;
            while (node.WaitingRequests.Count > 0)
                if (node.WaitingRequests.TryTake(out request) && !(request is HealthCheckRequest))
                    SendRequest(request);
        }

        protected bool SendRequest(IMemcacheRequest request)
        {
            var node = _locator.Locate(request.Key, _nodes);

            if (node == null)
            {
                if (_configuration.UnavaillablePolicy != Policy.Ignore)
                    throw new Exception("No nodes are available");
                else
                    return false;
            }

            var res = node.WaitingRequests.TryAdd(request, _configuration.EnqueueTimeout);
            if (!res && _configuration.QueueFullPolicy == Policy.Throw)
                throw new Exception("Queue is full");

            return res;
        }

        public bool Set(string key, byte[] message, TimeSpan expiration)
        {
            return SendRequest(new SetRequest { Key = key, Message = message, Expire = expiration });
        }

        public bool Get(string key, Action<byte[]> callback)
        {
            return SendRequest(new GetRequest { Key = key, Callback = callback });
        }

        public Task<byte[]> Get(string key)
        {
            var taskSource = new TaskCompletionSource<byte[]>();

            if (!SendRequest(new GetRequest { Key = key, Callback = taskSource.SetResult }))
                taskSource.SetResult(null);

            return taskSource.Task;
        }
    }
}
