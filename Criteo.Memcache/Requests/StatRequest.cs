using Criteo.Memcache.Headers;
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
using System.Text;

namespace Criteo.Memcache.Requests
{
    internal class StatRequest : MemcacheRequestBase, IMemcacheRequest
    {
        public Action<IDictionary<string, string>> Callback;

        public override int Replicas { get { return 0; } }

        private Dictionary<string, string> _result = null;

        public byte[] GetQueryBuffer()
        {
            var message = new byte[MemcacheRequestHeader.Size + (Key == null ? 0 : Key.Length)];
            new MemcacheRequestHeader(Opcode.Stat)
            {
                VBucket = VBucket,
                Opaque = RequestId,
                KeyLength = (ushort)(Key == null ? 0 : Key.Length),
            }.ToData(message);

            if (Key != null)
                Key.CopyTo(message, MemcacheRequestHeader.Size);

            return message;
        }

        public void HandleResponse(MemcacheResponseHeader header, byte[] key, byte[] extra, byte[] message)
        {
            if (key != null)
            {
                if (_result == null)
                    _result = new Dictionary<string, string>();

                var keyAsString = Encoding.UTF8.GetString(key);
                var messageAsString = message != null ? Encoding.UTF8.GetString(message) : null;
                _result.Add(keyAsString, messageAsString);
            }
            else if (Callback != null)
            {
                Callback(_result);
            }
        }

        public void Fail()
        {
            Callback(_result);
        }
    }
}
