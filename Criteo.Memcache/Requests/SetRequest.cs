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
        static private DateTime Epock = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private const uint RawDataFlag = 0xfa52;

        public string Key { get; set; }
        public byte[] Message { get; set; }
        public TimeSpan Expire { get; set; }
        public uint RequestId { get; set; }
        public uint Flags { get; set; }
        public Opcode RequestOpcode { get; set; }

        public Action<Status> CallBack { get; set; }

        public SetRequest()
        {
            RequestOpcode = Opcode.Set;
            Flags = RawDataFlag;
        }

        public byte[] GetQueryBuffer()
        {
            var keyAsBytes = UTF8Encoding.Default.GetBytes(Key);
            if (keyAsBytes.Length > ushort.MaxValue)
                throw new ArgumentException("The key is too long for the memcache binary protocol : " + Key);

            var requestHeader = new MemcacheRequestHeader(RequestOpcode)
            {
                KeyLength = (ushort)keyAsBytes.Length,
                ExtraLength = 2 * sizeof(uint),
                TotalBodyLength = (uint)(2 * sizeof(uint) + keyAsBytes.Length + (Message == null ? 0 : Message.Length)),
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.SIZE + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer, 0);
            buffer.CopyFrom(MemcacheRequestHeader.SIZE, Flags);

            uint expire;
            if (Expire.CompareTo(TimeSpan.FromDays(30)) < 0)
                expire = (uint)Expire.TotalSeconds;
            else
                expire = (uint)(DateTime.UtcNow.Add(Expire) - Epock).TotalSeconds;

            buffer.CopyFrom(MemcacheRequestHeader.SIZE + sizeof(uint), expire);
            keyAsBytes.CopyTo(buffer, MemcacheRequestHeader.SIZE + requestHeader.ExtraLength);
            if(Message != null)
                Message.CopyTo(buffer, MemcacheRequestHeader.SIZE + requestHeader.ExtraLength + keyAsBytes.Length);

            return buffer;
        }

        // nothing to do on set response
        public void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message)
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
