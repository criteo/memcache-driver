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
using System.IO.Pipes;

namespace Criteo.Memcache.UTest.Misc
{
    /// <summary>
    /// A unidirectional local data pipe.
    /// </summary>
    public class Aqueduct
    {
        public Stream In { get { return _server; } }

        public Stream Out { get { return _client; } }

        private AnonymousPipeServerStream _server;

        private AnonymousPipeClientStream _client;

        private bool _disposed;

        public Aqueduct()
        {
            _server = new AnonymousPipeServerStream(PipeDirection.Out, HandleInheritability.None);
            _client = new AnonymousPipeClientStream(PipeDirection.In, _server.GetClientHandleAsString());
        }

        public void Close()
        {
            if (!_disposed)
                lock (this)
                    if (!_disposed)
                    {
                        _server.WaitForPipeDrain();
                        _server.Dispose();
                        _client.Dispose();
                        _disposed = true;
                    }
        }

        public void Dispose()
        {
            if (!_disposed)
                lock (this)
                    if (!_disposed)
                    {
                        _server.Dispose();
                        _client.Dispose();
                        _disposed = true;
                    }
        }
    }
}