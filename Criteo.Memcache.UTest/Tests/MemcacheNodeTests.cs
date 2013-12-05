using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using NUnit.Framework;
using NUnit.Framework.Constraints;

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
            bool aliveness = true;
            var transportMocks = new List<TransportMock>();
            var config = new MemcacheClientConfiguration
            {
                DeadTimeout = TimeSpan.FromSeconds(1),
                TransportFactory = (_, __, r, s, _____) =>
                    {
                        var transport = new TransportMock(r) { IsAlive = aliveness, Setup = s };
                        transportMocks.Add(transport);
                        return transport;
                    },
                PoolSize = 2,
            };
            var node = new MemcacheNode(null, config);
            CollectionAssert.IsNotEmpty(transportMocks, "No transport has been created by the node");

            Assert.IsTrue(node.TrySend(new NoOpRequest(), Timeout.Infinite), "Unable to send a request throught the node");

            // creation of new transport will set them as dead
            aliveness = false;
            foreach (var transport in transportMocks)
                transport.IsAlive = false;

            Assert.IsFalse(node.TrySend(new NoOpRequest(), Timeout.Infinite), "The node did not failed with all transport deads");

            foreach (var transport in transportMocks)
                transport.IsAlive = true;

            Assert.IsFalse(node.IsDead, "The node is still dead, should be alive now !");
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
                var errorMutex = new ManualResetEventSlim(false);
                var callbackMutex = new ManualResetEventSlim(false);

                Exception expectedException = null;
                node.TransportError += e =>
                    {
                        Interlocked.Exchange<Exception>(ref expectedException, e);
                        errorMutex.Set();
                        Console.WriteLine(e);
                    };
                int nodeAliveCount = 0;
                node.NodeAlive += t => ++nodeAliveCount;
                int nodeDeadCount = 0;
                node.NodeDead += t => ++nodeDeadCount;

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
                            callbackMutex.Set();
                        },
                    }, 1000);
                // must wait before the next test because the TransportError event is fired after the callback call

                // Expected result :
                // * The callback must have been called before 1 sec
                // * The failure callback must have been called, so the received status must be InternalError
                // * An MemcacheException should have been raised by the receive due to the bad response
                // * The node should have exaclty one transport in its pool
                //      more mean that we added it twice after a failure, less means we didn't putted it back in the pool
                // * The node should not be seen has dead yet

                Assert.IsTrue(callbackMutex.Wait(1000), @"The 1st callback has not been received after 1 second");
                Assert.IsTrue(errorMutex.Wait(1000), @"The 1st error has not been received after 1 second");
                Assert.AreEqual(Status.InternalError, receivedStatus, @"A bad response has not sent an InternalError to the request callback");
                Assert.IsInstanceOf<Memcache.Exceptions.MemcacheException>(expectedException, @"A bad response has not triggered a transport error. Expected a MemcacheException.");
                Assert.AreEqual(1, node.PoolSize, @"A node contains more than the pool size sockets");
                Assert.AreEqual(0, nodeDeadCount, @"The node has been detected has dead before a new send has been made");

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
                expectedException = null;
                errorMutex.Reset();
                callbackMutex.Reset();
                receivedStatus = Status.NoError;

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
                            callbackMutex.Set();
                        },
                    }, 1000);

                // Expected result :
                // * An SocketException should have been raised by the send, since the previous receice has disconnected the socket
                // * The return must be true, because the request have been enqueued before the transport seen the socket died
                // * The failure callback must have been called, so the received status must be InternalError

                Assert.IsTrue(callbackMutex.Wait(1000), @"The 2nd callback has not been received after 1 second");
                Assert.IsTrue(errorMutex.Wait(1000), @"The 2nd error has not been received after 1 second");
                Assert.IsTrue(result, @"The first failed request should not see a false return");
                Assert.AreEqual(Status.InternalError, receivedStatus, @"The send operation should have detected that the socket is dead");
                Assert.IsInstanceOf<System.Net.Sockets.SocketException>(expectedException, @"A bad response has not triggered a transport error. Expected a SocketException.");

                // After a short delay, the transport should connect
                Assert.That(() => { return node.PoolSize; }, new DelayedConstraint(new EqualConstraint(1), 2000, 100), "After a while, the transport should manage to connect");

                expectedException = null;
                callbackMutex.Reset();
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
                            callbackMutex.Set();
                        },
                    }, 1000);

                // Expected result : everything should works fine now
                // * The return must be true, because the new transport should be available now
                // * No exception should have been raised
                // * The callback must have been called and with a NoError status

                Assert.IsTrue(result, @"The node has not been able to send a new request after a disconnection");
                Assert.IsTrue(callbackMutex.Wait(1000), @"The message has not been received after 1 second, case after reconnection");
                Assert.AreEqual(Status.NoError, receivedStatus, @"The response after a reconnection is still not NoError");
                Assert.IsNull(expectedException, "The request shouldn't have thrown an exception");
            }
        }
    }
}
