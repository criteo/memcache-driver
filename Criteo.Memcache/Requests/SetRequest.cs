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
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    class SetRequest : RedundantRequest, IMemcacheRequest
    {
        private const uint RawDataFlag = 0xfa52;

        public byte[] Message { get; set; }
        public uint Flags { get; set; }
        public Opcode RequestOpcode { get; set; }

        public Action<Status> CallBack { get; set; }

        private TimeSpan _expire { get; set; }

        public TimeSpan Expire
        {
            get
            {
                return _expire;
            }

            set
            {
                if (ExpirationTimeUtils.IsValid(value))
                    _expire = value;
                else
                    throw new ArgumentException("Invalid expiration time: " + value.ToString());
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

            var buffer = new byte[MemcacheRequestHeader.SIZE + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer, 0);
            buffer.CopyFrom(MemcacheRequestHeader.SIZE, Flags);
            buffer.CopyFrom(MemcacheRequestHeader.SIZE + sizeof(uint), ExpirationTimeUtils.MemcachedTTL(Expire));
            Key.CopyTo(buffer, MemcacheRequestHeader.SIZE + requestHeader.ExtraLength);
            if(Message != null)
                Message.CopyTo(buffer, MemcacheRequestHeader.SIZE + requestHeader.ExtraLength + Key.Length);

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
