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
using System.IO;
using System.Threading;

namespace Criteo.Memcache.Cluster.Couchbase
{
    internal class AsyncLinesStreamReader : IDisposable
    {
        private const char DelimiterChar = '\n';
        private const int DelimiterCount = 3;

        private readonly Stream _stream;
        private readonly byte[] _buffer;
        private MemoryStream _chunk;
        private int _delimiters;

        public TimeSpan PushCheckInterval { get; set; }
        public event Action<Stream> OnChunk;
        public event Action<Exception> OnError;

        public AsyncLinesStreamReader(Stream stream)
        {
            _stream = stream;
            _buffer = new byte[1024];
            _chunk = new MemoryStream();
            _delimiters = 0;

            PushCheckInterval = TimeSpan.FromMilliseconds(20);
        }

        public void StartReading()
        {
            try
            {
                _stream.BeginRead(_buffer, 0, _buffer.Length, EndRead, null);
            }
            catch (Exception e)
            {
                if (OnError != null)
                    OnError(e);
            }
        }

        protected void EndRead(IAsyncResult ar)
        {
            try
            {
                UnprotectedEndRead(ar);
            }
            catch (Exception e)
            {
                if (OnError != null)
                    OnError(e);
            }
        }

        protected void UnprotectedEndRead(IAsyncResult ar)
        {
            int byteCount = _stream.EndRead(ar);

            if (byteCount == 0)
            {
                StartReading();
                return;
            }

            // Check for complete chunks and call OnChunk if there are some
            var start = 0;
            for (var i = 0; i < byteCount; i++)
            {
                if (_buffer[i] == DelimiterChar)
                    _delimiters++;
                else
                    _delimiters = 0;

                if (_delimiters == DelimiterCount)
                {
                    _chunk.Write(_buffer, start, i - start - 2);

                    // MemoryStream reads from the current position, so we need to reset it
                    _chunk.Position = 0;
                    if (OnChunk != null)
                        OnChunk(_chunk);

                    _chunk = new MemoryStream();
                    _delimiters = 0;
                    start = i + 1;
                }
            }

            // Don't forget to append any remaining bytes to the current chunk
            if (start < byteCount)
                _chunk.Write(_buffer, start, byteCount - start);

            // Make sure the async loop goes on
            StartReading();
        }

        #region IDisposable
        public void Dispose()
        {
            _stream.Dispose();
        }
        #endregion
    }
}
