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
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

using Criteo.Memcache.Cluster.Couchbase;
using Criteo.Memcache.UTest.Misc;
using NUnit.Framework;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class AsyncLinesStreamReaderTests
    {
        private const string DELIMITER = "\n\n\n";

        private class TestCounters
        {
            private int _errors;
            private int _chunks;

            public TestCounters()
            {
                _errors = 0;
                _chunks = 0;
            }

            public int Errors { get { return _errors; } }

            public int Chunks { get { return _chunks; } }

            public void IncrementErrors()
            {
                Interlocked.Increment(ref _errors);
            }

            public void IncrementChunks()
            {
                Interlocked.Increment(ref _chunks);
            }
        }

        private Aqueduct _pipe;
        private TestCounters _counters;
        private AsyncLinesStreamReader _reader;

        [SetUp]
        public void SetUp()
        {
            var localCounters = new TestCounters();
            _counters = localCounters;

            _pipe = new Aqueduct();
            _reader = new AsyncLinesStreamReader(_pipe.Out);
            _reader.OnError += e => { localCounters.IncrementErrors(); Console.Error.WriteLine(e.Message); Console.Error.WriteLine(e.StackTrace); };
            _reader.OnChunk += _ => { localCounters.IncrementChunks(); };

            _reader.StartReading();
        }

        [TearDown]
        public void TearDown()
        {
            _reader.Dispose();
            _reader = null;

            _pipe.Close();
            _pipe = null;
        }

        [Test]
        public void TestNothingHappens()
        {
            // No delimiter
            SendRaw("Hello, world!");

            Assert.AreEqual(0, _counters.Errors, "errors");
            Assert.AreEqual(0, _counters.Chunks, "chunks");
        }

        [Test]
        public void TestError()
        {
            _pipe.Close();

            // Make sure that an asynchronous read happens
            Assert.That(() => _counters.Errors, Is.EqualTo(1).After(5000, 10), "errors");
            Assert.AreEqual(0, _counters.Chunks, "chunks");
        }

        [Test]
        public void TestSingleDelimiter()
        {
            string received = null;
            _reader.OnChunk += c => received = c;

            SendRaw(DELIMITER);

            Assert.That(() => received, Is.Not.Null.After(5000, 10), "chunk received");

            Assert.AreEqual(0, _counters.Errors, "errors");
            Assert.AreEqual(1, _counters.Chunks, "chunks");

            Assert.IsNullOrEmpty(received, "compare sent vs received");
        }

        [Test]
        public void TestSingleChunk()
        {
            const string sent = "Hello, world!";

            // Check that we receive exactly what was sent
            string received = null;
            _reader.OnChunk += c => received = c;

            Send(sent);

            Assert.That(() => received, Is.Not.Null.After(5000, 10), "chunk received");

            Assert.AreEqual(0, _counters.Errors, "errors");
            Assert.AreEqual(1, _counters.Chunks, "chunks");

            Assert.AreEqual(sent, received, "compare sent vs received");
        }

        [Test]
        public void TestSeveralChunks()
        {
            const string sentA = "Hello!", sentB = "World!";

            string received = null;
            _reader.OnChunk += c => received = c;

            // First chunk
            Send(sentA);

            Assert.That(() => received, Is.Not.Null.After(5000, 10), "first chunk received");
            var receivedA = received;

            // Second chunk
            received = null;
            Send(sentB);

            Assert.That(() => received, Is.Not.Null.After(5000, 10), "second chunk received");
            var receivedB = received;

            Assert.AreEqual(0, _counters.Errors, "errors");
            Assert.AreEqual(2, _counters.Chunks, "chunks");
            Assert.AreEqual(sentA, receivedA, "compare sent vs received (first chunk)");
            Assert.AreEqual(sentB, receivedB, "compare sent vs received (second chunk)");
        }

        private void Send(string str)
        {
            SendRaw(str + DELIMITER);
        }

        private void SendRaw(string str)
        {
            var data = Encoding.UTF8.GetBytes(str);

            _pipe.In.Write(data, 0, data.Length);
            _pipe.In.Flush();
        }
    }
}
