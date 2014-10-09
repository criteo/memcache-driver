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

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    class DeleteRequest : RedundantRequest, IMemcacheRequest
    {
        public Action<Status> CallBack { get; set; }

        public byte[] GetQueryBuffer()
        {
            var requestHeader = new MemcacheRequestHeader(Opcode.Delete)
            {
                KeyLength = (ushort)Key.Length,
                ExtraLength = 0,
                TotalBodyLength = (uint)Key.Length,
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.Size + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer);
            Key.CopyTo(buffer, MemcacheResponseHeader.Size);

            return buffer;
        }

        // nothing to do on set response
        public void HandleResponse(MemcacheResponseHeader header, byte[] key, byte[] extra, byte[] message)
        {
            if (CallCallback(header.Status) && CallBack != null)
                CallBack(header.Status);
        }

        public void Fail()
        {
            if (CallCallback(Status.InternalError) && CallBack != null)
                CallBack(Status.InternalError);
        }

        public override string ToString()
        {
            return Opcode.Delete.ToString() + ";Id=" + RequestId + ";Key=" + Key;
        }
    }
}
