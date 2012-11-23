using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Net;
using System.Threading;

using Criteo.MemcacheClient.Configuration;
using Criteo.MemcacheClient.Sockets;
using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.Node
{

    public class MemcacheNode : IMemcacheNode
    {
        public bool IsDead { get; private set; }

        private static SocketAllocator DefaultAllocator = (IPEndPoint endPoint, BlockingCollection<IMemcacheRequest> waitingRequests) => new MemcacheSocketThreaded(endPoint, waitingRequests);

        private BlockingCollection<IMemcacheRequest> _waitingRequests;
        private List<IMemcacheSocket> _clients;
        public BlockingCollection<IMemcacheRequest> WaitingRequests { get { return _waitingRequests; } }

        public event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheError
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

        public event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheResponse
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

        public event Action<MemcacheNode> NodeDead;
        public event Action<MemcacheNode> NodeFlush;

        private MemcacheClientConfiguration _configuration;

        public MemcacheNode(IPEndPoint endPoint, MemcacheClientConfiguration configuration)
        {
            _configuration = configuration;

            if (configuration.QueueLength > 0)
                _waitingRequests = new BlockingCollection<IMemcacheRequest>(configuration.QueueLength);
            else
                _waitingRequests = new BlockingCollection<IMemcacheRequest>();

            _clients = new List<IMemcacheSocket>(configuration.PoolSize);
            for (int i = 0; i < configuration.PoolSize; ++i)
            {
                var socket = (configuration.SocketFactory ?? DefaultAllocator)(endPoint, _waitingRequests);
                socket.MemcacheResponse += (_, __) => _requestRan = true;
                _clients.Add(socket);
            }

            IsDead = false;
            _requestRan = false;
            _stuckCount = 0;
            _monitoring = new Timer(Monitor, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        bool _requestRan;
        int _stuckCount;
        private void Monitor(object dummy)
        {
            if (IsDead)
            {
                if (NodeFlush != null)
                    NodeFlush(this);

                var noOp = new HealthCheckRequest { Callback = _ => IsDead = false };
                WaitingRequests.Add(noOp);
            }
            else if (!_requestRan && WaitingRequests.Count > 0
                && ++_stuckCount > _configuration.DeadTimeout.TotalSeconds)
                // no request has been executed and the queue is not empty => we are stuck
                    MarkAsDead();
            _requestRan = false;
        }

        private void MarkAsDead()
        {
            // flag the node as dead
            IsDead = true;
            // flush the queue
            if (NodeFlush != null)
                NodeFlush(this);
            if (NodeDead != null)
                NodeDead(this);
        }

        private Timer _monitoring;
    }
}
