using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Net;
using System.Threading;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Transport;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Node
{

    internal class MemcacheNode : IMemcacheNode, IMemcacheRequestsQueue
    {
        public bool IsDead { get; private set; }

        private static SocketAllocator DefaultAllocator = (endPoint, authenticator, nodeQueue) => new MemcacheSocketThreadedRead(endPoint, nodeQueue as IMemcacheRequestsQueue, authenticator);

        private BlockingCollection<IMemcacheRequest> _waitingRequests;
        private List<IMemcacheTransport> _clients;
        private Action<IMemcacheRequest> _requeueRequest;
        private Timer _monitoring;
        private bool _requestRan;
        private int _stuckCount;
        private MemcacheClientConfiguration _configuration;
        private IPEndPoint _endPoint;

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError
        {
            add
            {
                foreach (var client in _clients)
                    client.MemcacheError += value;
            }
            remove
            {
                foreach (var client in _clients)
                    client.MemcacheError -= value;
            }
        }

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse
        {
            add
            {
                foreach (var client in _clients)
                    client.MemcacheResponse += value;
            }
            remove
            {
                foreach (var client in _clients)
                    client.MemcacheResponse -= value;
            }
        }

        public event Action<Exception> TransportError
        {
            add
            {
                foreach (var client in _clients)
                    client.TransportError += value;
            }
            remove
            {
                foreach (var client in _clients)
                    client.TransportError -= value;
            }
        }

        public event Action<IMemcacheNode> NodeDead;

        public IPEndPoint EndPoint
        {
            get { return _endPoint; }
        }

        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="endPoint">Ip address and port of the node</param>
        /// <param name="configuration">Configuration object</param>
        /// <param name="requeueRequest">Delegate used to requeue pending requests when the node deads</param>
        public MemcacheNode(IPEndPoint endPoint, MemcacheClientConfiguration configuration, Action<IMemcacheRequest> requeueRequest)
        {
            _requeueRequest = requeueRequest;
            _configuration = configuration;
            _endPoint = endPoint;

            if (configuration.QueueLength > 0)
                _waitingRequests = new BlockingCollection<IMemcacheRequest>(configuration.QueueLength);
            else
                _waitingRequests = new BlockingCollection<IMemcacheRequest>();

            _clients = new List<IMemcacheTransport>(configuration.PoolSize);
            for (int i = 0; i < configuration.PoolSize; ++i)
            {
                var socket = (configuration.SocketFactory ?? DefaultAllocator)(endPoint, configuration.Authenticator, this);
                socket.MemcacheResponse += (_, __) => _requestRan = true;
                _clients.Add(socket);
            }

            IsDead = false;
            _requestRan = false;
            _stuckCount = 0;
            _monitoring = new Timer(Monitor, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        /// <summary>
        /// See interface comments
        /// </summary>
        /// <param name="request" />
        /// <param name="timeout" />
        /// <returns />
        public bool TrySend(IMemcacheRequest request, int timeout)
        {
            return _waitingRequests.TryAdd(request, timeout);
        }

        /// <summary>
        /// The method executed by the monitoring timer
        /// It checks if the node is dead when it's up, and if it's up again when it's dead
        /// </summary>
        /// <param name="dummy" />
        private void Monitor(object dummy)
        {
            if (!IsDead)
            {
                if (!_requestRan && _waitingRequests.Count > 0)
                {
                    // no request ran and the queue is not empty => increment the stuck counter
                    ++_stuckCount;
                    if (_stuckCount >= _configuration.DeadTimeout.TotalSeconds)
                    {
                        // we are stuck for too long time, the node is dead
                        IsDead = true;
                        FlushNode();

                        if (NodeDead != null)
                            NodeDead(this);
                    }
                }
                else
                {
                    // reset the stuck counter
                    _stuckCount = 0;
                }
            }
            if (IsDead)
            {
                FlushNode();

                var noOp = new HealthCheckRequest { Callback = _ => IsDead = false };
                _waitingRequests.Add(noOp);
            }
            _requestRan = false;
        }

        /// <summary>
        /// Flush the content of the internal queue to eventually requeue them in unother node
        /// </summary>
        private void FlushNode()
        {
            IMemcacheRequest req;
            while (_waitingRequests.TryTake(out req))
                if (!(req is HealthCheckRequest))
                    _requeueRequest(req);
        }

        IMemcacheRequest IMemcacheRequestsQueue.Take()
        {
            return _waitingRequests.Take();
        }

        bool IMemcacheRequestsQueue.TryTake(out IMemcacheRequest request, int timeout)
        {
            return _waitingRequests.TryTake(out request, timeout);
        }

        void IMemcacheRequestsQueue.Add(IMemcacheRequest request)
        {
            _waitingRequests.Add(request);
        }

        public override string ToString()
        {
            return _endPoint.Address.ToString() + ":" + _endPoint.Port;
        }

        public void Dispose()
        {
            foreach (var client in _clients)
                client.Dispose();
        }
    }
}
