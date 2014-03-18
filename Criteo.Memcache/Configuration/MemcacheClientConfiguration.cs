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
using System.Net;
using System.Threading;

using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Locator;
using Criteo.Memcache.Node;
using Criteo.Memcache.Transport;

namespace Criteo.Memcache.Configuration
{
    /// <summary>
    /// If you want to implement a different transport layer, use this delegate to inject it in the client
    /// </summary>
    /// <param name="endPoint">Remote Memcached server listening endpoint</param>
    /// <param name="clientConfig">The MemcacheClientConfiguration passed to the client at construction</param>
    /// <param name="registerEvents">Delegate used to register the transport event to the node it belongs to</param>
    /// <param name="transportAvailable">Delegate used to return the transport to the node it belongs to,
    /// after a request has been succesfully set or after the connection has been succesfully established</param>
    /// <param name="autoConnect">if set to true, it will set a timer to try to connect while connection fails
    /// else it will lazily and synchronously connect at first request sent</param>
    /// <param name="ongoingDispose">Object for signaling that a dispose is requested</param>
    /// <returns>The allocated transport</returns>
    public delegate IMemcacheTransport TransportAllocator(
        EndPoint endPoint,
        MemcacheClientConfiguration clientConfig,
        Action<IMemcacheTransport> registerEvents,
        Action<IMemcacheTransport> transportAvailable,
        bool autoConnect,
        Func<bool> nodeClosing);

    /// <summary>
    /// If you want to implement your own nodes, then use this delegate to inject it in the client
    /// </summary>
    /// <param name="endPoint">Remote Memcached server listening endpoint</param>
    /// <param name="configuration">The MemcacheClientConfiguration passed to the client at construction</param>
    /// <param name="ongoingDispose">Object for signaling that a dispose is requested</param>
    /// <returns>The allocated node</returns>
    public delegate IMemcacheNode NodeAllocator(EndPoint endPoint, MemcacheClientConfiguration configuration);

    /// <summary>
    /// Delegate for Sasl authentication
    /// </summary>
    /// <param name="zone">Sasl zone
    /// unused at this moment, but part of the sasl specification
    /// you can pass null</param>
    /// <param name="user">Sasl user
    /// for Couchbase it must be the name of the bucket</param>
    /// <param name="password">Sasl password</param>
    /// <returns>The authenticator that will be used for every new socket connection</returns>
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

        public static AuthenticatorAllocator SaslPlainAuthenticatorFactory =
            (zone, user, password) => new SaslPlainTextAuthenticator { Zone = zone, User = user, Password = password };

        #endregion factories

        private readonly IList<IPEndPoint> _nodesEndPoints = new List<IPEndPoint>();
        public IList<IPEndPoint> NodesEndPoints { get { return _nodesEndPoints;} }

        public INodeLocator NodeLocator { get; set; }
        public TransportAllocator TransportFactory { get; set; }
        public NodeAllocator NodeFactory { get; set; }
        public IMemcacheAuthenticator Authenticator { get; set; }

        public int PoolSize { get; set; }
        public int QueueLength { get; set; }
        public int QueueTimeout { get; set; }
        public TimeSpan TransportConnectTimerPeriod { get; set; }
        public int TransportReceiveBufferSize { get; set; }
        public int TransportSendBufferSize { get; set; }
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
            QueueLength = 0;
            TransportConnectTimerPeriod = TimeSpan.FromMilliseconds(1000);
            TransportReceiveBufferSize = 2 << 15;   // 32kB
            TransportSendBufferSize = 2 << 15;      // 32kB
            Replicas = 0;
        }
    }
}