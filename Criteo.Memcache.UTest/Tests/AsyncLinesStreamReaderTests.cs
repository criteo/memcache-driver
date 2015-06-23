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

        private TestCounters _counters;
        private Stream _stream;
        private AsyncLinesStreamReader _reader;
        private MemoryStream _memoryStream;

        [SetUp]
        public void SetUp()
        {
            var localCounters = new TestCounters();
            _counters = localCounters;

            _memoryStream = new MemoryStream();
            _stream = Stream.Synchronized(_memoryStream);

            _reader = new AsyncLinesStreamReader(_stream);
            _reader.OnError += e => { localCounters.IncrementErrors(); Debug.WriteLine(e.Message); Debug.WriteLine(e.StackTrace); };
            _reader.OnChunk += _ => { localCounters.IncrementChunks(); };
        }

        [TearDown]
        public void TearDown()
        {
            _reader.Dispose();
            _reader = null;

            _stream.Dispose();
            _stream = null;
        }

        [Test]
        public void TestNothingHappens()
        {
            SendAndTriggerRead("Hello, world!");

            Assert.AreEqual(0, _counters.Errors, "errors");
            Assert.AreEqual(0, _counters.Chunks, "chunks");
        }

        [Test]
        public void TestError()
        {
            _stream.Close();

            if (_stream.CanRead) //special case for mono (SynchronizedStream.Close does not work)
            {
                _memoryStream.Close();
                Assert.False(_stream.CanRead);
            }

            // Trigger read
            _reader.StartReading();

            // Make sure that an asynchronous read happens
            Assert.That(() => _counters.Errors, Is.EqualTo(1).After(5000, 10), "errors");
            Assert.AreEqual(0, _counters.Chunks, "chunks");
        }

        [Test]
        public void TestSingleDelimiter()
        {
            Stream receivedStream = null;
            _reader.OnChunk += c => receivedStream = c;

            SendAndTriggerRead(DELIMITER);
            Assert.That(() => receivedStream, Is.Not.Null.After(5000, 10), "chunk received");

            Assert.AreEqual(0, _counters.Errors, "errors");
            Assert.AreEqual(1, _counters.Chunks, "chunks");

            var received = new StreamReader(receivedStream).ReadToEnd();
            Assert.IsNullOrEmpty(received, "compare sent vs received");
        }

        [Test]
        public void TestSingleChunk()
        {
            const string sent = "Hello, world!";

            // Check that we receive exactly what was sent
            Stream receivedStream = null;
            _reader.OnChunk += c => receivedStream = c;

            SendAndTriggerRead(sent + DELIMITER);
            Assert.That(() => receivedStream, Is.Not.Null.After(5000, 10), "chunk received");

            Assert.AreEqual(0, _counters.Errors, "errors");
            Assert.AreEqual(1, _counters.Chunks, "chunks");

            var received = new StreamReader(receivedStream).ReadToEnd();
            Assert.AreEqual(sent, received, "compare sent vs received");
        }

        [Test]
        public void TestSeveralChunks()
        {
            const string sentA = "Hello!", sentB = "World!";

            // Check that we receive exactly what was sent
            Stream receivedStreamA = null;
            Stream receivedStreamB = null;
            _reader.OnChunk += c => { if (receivedStreamA == null) { receivedStreamA = c; } else { receivedStreamB = c; } };

            SendAndTriggerRead(sentA + DELIMITER + sentB + DELIMITER);
            Assert.That(() => receivedStreamB, Is.Not.Null.After(5000, 10), "both chunks received");

            Assert.AreEqual(0, _counters.Errors, "errors");
            Assert.AreEqual(2, _counters.Chunks, "chunks");

            var receivedA = new StreamReader(receivedStreamA).ReadToEnd();
            Assert.AreEqual(sentA, receivedA, "compare sent vs received (first chunk)");
            var receivedB = new StreamReader(receivedStreamB).ReadToEnd();
            Assert.AreEqual(sentB, receivedB, "compare sent vs received (second chunk)");
        }

        /// <summary>
        /// To be called only once per test!
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        private void SendAndTriggerRead(string str)
        {
            var data = Encoding.UTF8.GetBytes(str);

            _stream.Write(data, 0, data.Length);
            _stream.Flush();
            _stream.Position = 0;

            _reader.StartReading();
        }
    }
}