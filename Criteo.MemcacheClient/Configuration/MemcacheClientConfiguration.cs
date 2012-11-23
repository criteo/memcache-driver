using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;

using Criteo.MemcacheClient.Requests;
using Criteo.MemcacheClient.Sockets;
using Criteo.MemcacheClient.Node;

namespace Criteo.MemcacheClient.Configuration
{
    public enum Policy
    {
        Throw,
        Ignore,
    }

    public delegate IMemcacheSocket SocketAllocator(IPEndPoint endPoint, BlockingCollection<IMemcacheRequest> waitingRequests);
    public delegate IMemcacheNode NodeAllocator(IPEndPoint endPoint, MemcacheClientConfiguration configuration);

    public class MemcacheClientConfiguration
    {
        private IList<IPEndPoint> _nodesEndPoints = new List<IPEndPoint>();
        public IList<IPEndPoint> NodesEndPoints { get { return _nodesEndPoints;} }

        public INodeLocator NodeLocator { get; set; }
        public SocketAllocator SocketFactory { get; set; }
        public NodeAllocator NodeFactory { get; set; }

        public Policy UnavaillablePolicy { get; set; }
        public Policy QueueFullPolicy { get; set; }
        public int EnqueueTimeout { get; set; }
        public int PoolSize { get; set; }
        public int QueueLength { get; set; }
        public TimeSpan DeadTimeout { get; set; }
        public TimeSpan SocketTimeout { get; set; }

        public MemcacheClientConfiguration()
        {
            PoolSize = 2;
            DeadTimeout = TimeSpan.FromSeconds(15);
            SocketTimeout = TimeSpan.FromMilliseconds(200);
            UnavaillablePolicy = Policy.Ignore;
            QueueFullPolicy = Policy.Ignore;
            EnqueueTimeout = Timeout.Infinite;
        }
    }
}
