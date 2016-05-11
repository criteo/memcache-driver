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

using Criteo.Memcache.Exceptions;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    internal class GetRequest : RedundantRequest, IRedundantRequest, ICouchbaseRequest
    {
        private TimeSpan _expire;

        public Action<Status, byte[]> CallBack { get; set; }
        public Opcode RequestOpcode { get; set; }
        public uint Flag { get; private set; }
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

        public GetRequest(CallBackPolicy callBackPolicy)
            : base(callBackPolicy)
        {
            // set the default opcode to get
            RequestOpcode = Opcode.Get;

            // Only relevant for Get-And-Touch command
            _expire = ExpirationTimeUtils.Infinite;
        }

        public byte[] GetQueryBuffer()
        {
            if (RequestOpcode != Opcode.Get && RequestOpcode != Opcode.GetK && RequestOpcode != Opcode.GAT && RequestOpcode != Opcode.ReplicaRead)
                throw new MemcacheException("Get request only supports Get, GetK, GAT or ReplicaRead opcodes");

            int extraLength = RequestOpcode == Opcode.GAT ? sizeof(uint) : 0;

            var requestHeader = new MemcacheRequestHeader(RequestOpcode)
            {
                VBucket = VBucket,
                KeyLength = (ushort)Key.Length,
                ExtraLength = (byte)extraLength,
                TotalBodyLength = (uint)(extraLength + Key.Length),
                Opaque = RequestId,
            };

            var buffer = new byte[MemcacheRequestHeader.Size + requestHeader.TotalBodyLength];
            requestHeader.ToData(buffer);

            // in case of Get and Touch, post the new TTL in extra
            if (RequestOpcode == Opcode.GAT)
                buffer.CopyFrom(MemcacheRequestHeader.Size + sizeof(uint), ExpirationTimeUtils.MemcachedTTL(Expire));

            Key.CopyTo(buffer, extraLength + MemcacheRequestHeader.Size);

            return buffer;
        }

        public void HandleResponse(MemcacheResponseHeader header, byte[] key, byte[] extra, byte[] message)
        {
            if (header.Status == Status.NoError)
            {
                if (extra == null || extra.Length == 0)
                    throw new MemcacheException("The get command flag is not present !");

                if (extra.Length != 4)
                    throw new MemcacheException("The get command flag is the wrong size !");

                Flag = extra.CopyToUInt(0);
            }

            if (CallCallback(header.Status) && CallBack != null)
                CallBack(GetResult(), message);
        }

        public void Fail()
        {
            if (CallCallback(Status.InternalError) && CallBack != null)
                CallBack(GetResult(), null);
        }

        public override string ToString()
        {
            return RequestOpcode.ToString() + ";Id=" + RequestId + ";Key=" + Key;
        }
    }
}
