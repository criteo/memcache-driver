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
using System.IO;
using System.Text;
using Criteo.Memcache.Cluster.Couchbase;
using NUnit.Framework;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class AsyncLinesStreamReaderTests
    {
        private const string Delimiter = "\n\n\n";

        private int _errors;
        private int _chunks;
        private Stream _stream;
        private AsyncLinesStreamReader _reader;
        private MemoryStream _memoryStream;

        [SetUp]
        public void SetUp()
        {
            _errors = _chunks = 0;

            _memoryStream = new MemoryStream();
            _stream = Stream.Synchronized(_memoryStream);

            _reader = new AsyncLinesStreamReader(_stream);
            _reader.OnError += _ => { _errors++; };
            _reader.OnChunk += _ => { _chunks++; };

            _reader.StartReading();
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
            Send("Hello, world!");

            Assert.AreEqual(0, _errors);
            Assert.AreEqual(0, _chunks);
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

            // Make sure that an asynchronous read happens
            Assert.That(() => _errors, Is.EqualTo(1).After(5000, 10));
            Assert.AreEqual(0, _chunks);
        }

        [Test]
        public void TestSingleDelimiter()
        {
            Send(Delimiter);

            Assert.AreEqual(0, _errors);
            Assert.AreEqual(1, _chunks);
        }

        [Test]
        public void TestSingleChunk()
        {
            const string sent = "Hello, world!";

            // Check that we receive exactly what was sent
            Stream receivedStream = null;
            _reader.OnChunk += c => receivedStream = c;

            Send(sent + Delimiter);
            Assert.IsNotNull(receivedStream);

            var received = new StreamReader(receivedStream).ReadToEnd();
            Assert.AreEqual(sent, received);
        }

        [Test]
        public void TestSeveralChunks()
        {
            const string sentA = "Hello!", sentB = "World!";

            // Check that we receive exactly what was sent
            Stream receivedStream = null;
            _reader.OnChunk += c => receivedStream = c;

            Send(sentA + Delimiter + sentB + Delimiter);
            Assert.That(() => receivedStream, Is.Not.Null.After(5000, 10));

            // Check that the last received chunk is the one we expect
            var received = new StreamReader(receivedStream).ReadToEnd();
            Assert.AreEqual(0, _errors);
            Assert.AreEqual(2, _chunks);
            Assert.AreEqual(sentB, received);
        }

        private void Send(string str)
        {
            var data = Encoding.UTF8.GetBytes(str);

            _stream.Write(data, 0, data.Length);
            _stream.Position = 0;

            // Make sure the data has been read
            Assert.That(() => _stream.Position, Is.AtLeast(data.Length).After(5000, 10));
        }
    }
}
