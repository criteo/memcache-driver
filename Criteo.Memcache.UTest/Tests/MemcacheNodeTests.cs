using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using NUnit.Framework;

using Criteo.Memcache.Configuration;
using Criteo.Memcache.Node;
using Criteo.Memcache.UTest.Mocks;
using Criteo.Memcache.Requests;
using System.Net;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.UTest.Tests
{
    /// <summary>
    /// Test around the MemcacheNode object
    /// </summary>
    [TestFixture]
    public class MemcacheNodeTests
    {
        [Test]
        public void SyncNodeDeadDetection()
        {
            var transportMocks = new List<TransportMock>();
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                SynchronousTransportFactory = (_, __, ___, ____, s) => 
                    {
                        var transport = new TransportMock { IsDead = false, Setup = s };
                        transportMocks.Add(transport);
                        return transport;
                    },
                PoolSize = 2,
            };
            var node = new MemcacheNode(null, config);
            CollectionAssert.IsNotEmpty(transportMocks, "No transport has been created by the node");
            
            Assert.IsTrue(node.TrySend(new NoOpRequest(), Timeout.Infinite), "Unable to send a request throught the node");

            foreach (var transport in transportMocks)
                transport.IsDead = true;

            Assert.IsFalse(node.TrySend(new NoOpRequest(), Timeout.Infinite), "The node did not failed with all transport deads");

            foreach (var transport in transportMocks)
                transport.IsDead = false;

            Assert.AreEqual(false, node.IsDead, "The node is still dead, should be alive now !");
            Assert.IsTrue(node.TrySend(new NoOpRequest(), Timeout.Infinite), "Unable to send a request throught the node after it's alive");
        }

        private IPAddress LOCALHOST = new IPAddress(new byte[] { 127, 0, 0, 1 });

        [Test]
        public void ReceiveFailConsistency()
        {
            var endpoint = new IPEndPoint(LOCALHOST, 12347);

            using (var serverMock = new ServerMock(endpoint))
            {
                serverMock.ResponseBody = new byte[24];

                var config = new MemcacheClientConfiguration
                {
                    PoolSize = 1,
                };

                var node = new Memcache.Node.MemcacheNode(endpoint, config);

                Exception expectedException = null;
                node.TransportError += e => { expectedException = e; Console.WriteLine(e); };
                bool nodeAlive = true;
                node.NodeAlive += t => nodeAlive = true;
                node.NodeDead += t => nodeAlive = false;

                var mutex = new ManualResetEventSlim(false);
                Status receivedStatus = Status.NoError;
                node.TrySend(
                    new SetRequest 
                    {
                        Code = Opcode.Set,
                        RequestId = 1, 
                        Expire = TimeSpan.FromSeconds(1),
                        Key = @"Key", 
                        Message = new byte[] { 0, 1, 2, 3, 4 }, 
                        CallBack = s => 
                        { 
                            receivedStatus = s;
                            mutex.Set();
                        }, 
                    }, 1000);
                Assert.IsTrue(mutex.Wait(1000), @"The message has not been received after 1 second");
                Assert.IsNotNull(expectedException, @"A bad response has not triggered a transport error");
                Assert.AreEqual(Status.InternalError, receivedStatus, @"A bad response has not sent an InternalError to the request callback");
                Assert.AreEqual(1, node.PoolSize, @"A node contains more than the pool size sockets");
                Assert.IsTrue(nodeAlive, @"The node has been detected has dead before a new send has been made");

                serverMock.ResponseHeader = new MemcacheResponseHeader
                {
                    Cas = 8,
                    DataType = 12,
                    ExtraLength = 0,
                    KeyLength = 0,
                    // must be the same or it will crash : TODO add a test to ensure we detect this fail
                    Opaque = 1,
                    Status = Status.NoError,
                    Opcode = Opcode.Set,
                    TotalBodyLength = 0,
                };
                serverMock.ResponseBody = null;

                var result = node.TrySend(
                    new SetRequest
                    {
                        Code = Opcode.Set,
                        RequestId = 1,
                        Expire = TimeSpan.FromSeconds(1),
                        Key = @"Key",
                        Message = new byte[] { 0, 1, 2, 3, 4 },
                        CallBack = s =>
                        {
                            receivedStatus = s;
                            mutex.Set();
                        },
                    }, 1000);
                Assert.IsFalse(result, @"The node has been able to send a new request after a disconnection");
                Assert.AreEqual(Status.InternalError, receivedStatus, @"The send operation should have detected that the socket is dead");
                Assert.IsFalse(nodeAlive, @"The node has been detected has alive but should be seen has dead");
                Thread.Sleep(1500);

                mutex.Reset();
                result = node.TrySend(
                    new SetRequest
                    {
                        Code = Opcode.Set,
                        RequestId = 1,
                        Expire = TimeSpan.FromSeconds(1),
                        Key = @"Key",
                        Message = new byte[] { 0, 1, 2, 3, 4 },
                        CallBack = s =>
                        {
                            receivedStatus = s;
                            mutex.Set();
                        },
                    }, 1000);

                Assert.IsTrue(result, @"The node has not been able to send a new request after a disconnection");
                Assert.IsTrue(mutex.Wait(1000000), @"The message has not been received after 1 second, case after reconnection");
                Assert.AreEqual(Status.NoError, receivedStatus, @"The response after a reconnection is still not NoError");
                Assert.IsTrue(nodeAlive, @"The node has been detected has dead after reconnection complete");
            }
        }
    }
}
