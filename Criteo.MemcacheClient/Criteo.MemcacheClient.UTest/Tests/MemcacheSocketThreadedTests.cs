using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;

using NUnit.Framework;

using Criteo.MemcacheClient.Requests;
using Criteo.MemcacheClient.Sockets;
using Criteo.MemcacheClient.Node;

using Criteo.MemcacheClient.UTest.Mocks;


namespace Criteo.MemcacheClient.UTest.Tests
{
    /// <summary>
    /// Test around the MemcacheSocketThreaded object
    /// Not working yet !
    /// </summary>
    [TestFixture]
    public class MemcacheSocketThreadedTests
    {
        [Test]
        public void MemcacheSocketThreadedTest()
        {
            var localhost = new IPEndPoint(new IPAddress(new byte[] {127, 0, 0, 1}), 11211);

            var serverMock = new ServerMock(localhost);
            var queue = new System.Collections.Concurrent.BlockingCollection<IMemcacheRequest>() as IMemcacheNodeQueue;
            var socket = new MemcacheSocketThreaded(localhost, queue);
        }
    }
}
