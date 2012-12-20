using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Sockets;
using Criteo.Memcache.Node;
using Criteo.Memcache.Locator;
using Criteo.Memcache.Authenticators;

namespace Criteo.Memcache.Configuration
{
    public enum Policy
    {
        Throw,
        Ignore,
    }

    public enum RequeuePolicy
    {
        Requeue,
        Ignore,
    }

    public delegate IMemcacheSocket SocketAllocator(IPEndPoint endPoint, IMemcacheNodeQueue nodeQueue, IMemcacheAuthenticator authenticator);
    public delegate IMemcacheNode NodeAllocator(IPEndPoint endPoint, MemcacheClientConfiguration configuration, Action<IMemcacheRequest> requeueRequest);

    public class MemcacheClientConfiguration
    {
        private IList<IPEndPoint> _nodesEndPoints = new List<IPEndPoint>();
        public IList<IPEndPoint> NodesEndPoints { get { return _nodesEndPoints;} }

        public INodeLocator NodeLocator { get; set; }
        public SocketAllocator SocketFactory { get; set; }
        public NodeAllocator NodeFactory { get; set; }
        public IMemcacheAuthenticator Authenticator { get; set; }

        public Policy UnavaillablePolicy { get; set; }
        public Policy QueueFullPolicy { get; set; }
        public RequeuePolicy NodeDeadPolicy { get; set; }
        public int EnqueueTimeout { get; set; }
        public int PoolSize { get; set; }
        public int QueueLength { get; set; }
        public TimeSpan DeadTimeout { get; set; }
        public TimeSpan SocketTimeout { get; set; }

        public MemcacheClientConfiguration()
        {
            Authenticator = null;
            PoolSize = 2;
            DeadTimeout = TimeSpan.FromSeconds(15);
            SocketTimeout = TimeSpan.FromMilliseconds(200);
            UnavaillablePolicy = Policy.Ignore;
            QueueFullPolicy = Policy.Ignore;
            EnqueueTimeout = Timeout.Infinite;
            NodeDeadPolicy = RequeuePolicy.Requeue;
        }
    }
}
