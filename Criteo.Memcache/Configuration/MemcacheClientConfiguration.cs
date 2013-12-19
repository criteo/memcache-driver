using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Locator;
using Criteo.Memcache.Node;
using Criteo.Memcache.Transport;

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

    public delegate IMemcacheTransport TransportAllocator(EndPoint endPoint, MemcacheClientConfiguration clientConfig, Action<IMemcacheTransport> registerEvents, Action<IMemcacheTransport> transportAvailable, bool autoConnect);

    public delegate IMemcacheNode NodeAllocator(IPEndPoint endPoint, MemcacheClientConfiguration configuration);

    public delegate IMemcacheAuthenticator AuthenticatorAllocator(string zone, string user, string password);

    public class MemcacheClientConfiguration
    {
        #region factories

        internal static NodeAllocator DefaultNodeFactory =
            (endPoint, configuration) => new MemcacheNode(endPoint, configuration);

        internal static Func<INodeLocator> DefaultLocatorFactory =
            () => new KetamaLocator();

        public static Func<string, INodeLocator> KetamaLocatorFactory =
            hashName => new KetamaLocator(hashName);

        public static Func<INodeLocator> RoundRobinLocatorFactory =
            () => new RoundRobinLocator();

        public static Func<string, string, string, IMemcacheAuthenticator> SaslPlainAuthenticatorFactory =
            (zone, user, password) => new SaslPlainTextAuthenticator { Zone = zone, User = user, Password = password };

        #endregion factories

        private readonly IList<IPEndPoint> _nodesEndPoints = new List<IPEndPoint>();
        public IList<IPEndPoint> NodesEndPoints { get { return _nodesEndPoints;} }

        public INodeLocator NodeLocator { get; set; }
        public TransportAllocator TransportFactory { get; set; }
        public NodeAllocator NodeFactory { get; set; }
        public IMemcacheAuthenticator Authenticator { get; set; }

        public RequeuePolicy NodeDeadPolicy { get; set; }
        public int QueueTimeout { get; set; }
        public int PoolSize { get; set; }
        public int QueueLength { get; set; }
        public int TransportQueueLength { get; set; }       // Zero for unbounded queue size
        public int TransportQueueTimeout { get; set; }
        public TimeSpan DeadTimeout { get; set; }
        public TimeSpan SocketTimeout { get; set; }
        public int Replicas { get; set; }

        public MemcacheClientConfiguration()
        {
            Authenticator = null;
            PoolSize = 2;
            DeadTimeout = TimeSpan.FromSeconds(15);
            SocketTimeout = TimeSpan.FromMilliseconds(200);
            QueueTimeout = Timeout.Infinite;
            NodeDeadPolicy = RequeuePolicy.Ignore;
            TransportQueueLength = 0;
            TransportQueueTimeout = Timeout.Infinite;
            Replicas = 0;
        }
    }
}
