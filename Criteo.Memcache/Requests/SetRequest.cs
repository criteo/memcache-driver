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
    class SetRequest : RedundantRequest, IMemcacheRequest
    {
        private const uint RawDataFlag = 0xfa52;
        private TimeSpan _expire;

        public byte[] Message { get; set; }
        public uint Flags { get; set; }
        public Opcode RequestOpcode { get; set; }
        public Action<Status> CallBack { get; set; }
        public TimeSpan Expire
        {
            get
            {
                return _expire;
            }

            set
            {
                if (!ExpirationTimeUtils.IsValid(value))
                    throw new ArgumentException("Invalid expiration time: " + value.ToString());

                _expire = value;
            }
        }

        public SetRequest()
        {
            RequestOpcode = Opcode.Set;
            Flags = RawDataFlag;
            _expire = ExpirationTimeUtils.Infinite;
        }

        public byte[] GetQueryBuffer()
        {
            var requestHeader = new MemcacheRequestHeader(RequestOpcode)
            {
                KeyLength = (ushort)Key.Length,
                ExtraLength = 2 * sizeof(uint),
                TotalBodyLength = (uint)(2 * sizeof(uint) + Key.Length + (Message == null ? 0 : Message.Length)),
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.Size + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer);
            buffer.CopyFrom(MemcacheRequestHeader.Size, Flags);
            buffer.CopyFrom(MemcacheRequestHeader.Size + sizeof(uint), ExpirationTimeUtils.MemcachedTTL(Expire));
            Key.CopyTo(buffer, MemcacheRequestHeader.Size + requestHeader.ExtraLength);
            if (Message != null)
                Message.CopyTo(buffer, MemcacheRequestHeader.Size + requestHeader.ExtraLength + Key.Length);

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
            return RequestOpcode.ToString() + ";Id=" + RequestId + ";Key=" + Key;
        }
    }
}
