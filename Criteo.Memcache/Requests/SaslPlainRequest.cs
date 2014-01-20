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
﻿using System;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    internal class SaslPlainRequest : IMemcacheRequest
    {
        public uint RequestId
        {
            get { return 0; }
            set { return; }
        }

        public string Key
        {
            get { return "PLAIN"; }
            set { return; }
        }

        public int Replicas
        {
            get { return 0; }
            set { return; }
        }

        public string Zone { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public Action<Status> Callback { get; set; }

        public byte[] GetQueryBuffer()
        {
            var key = UTF8Encoding.Default.GetBytes(Key);
            var data = Encoding.UTF8.GetBytes(Zone + "\0" + User + "\0" + Password);

            var header = new MemcacheRequestHeader(Opcode.StartAuth)
            {
                ExtraLength = 0,
                KeyLength = (ushort)key.Length,
                TotalBodyLength = (uint)(key.Length + data.Length),
            };

            var message = new byte[MemcacheRequestHeader.SIZE + header.TotalBodyLength];
            header.ToData(message);
            Array.Copy(key, 0, message, MemcacheRequestHeader.SIZE, key.Length);
            Array.Copy(data, 0, message, MemcacheRequestHeader.SIZE + key.Length, data.Length);

            return message;
        }

        public void HandleResponse(MemcacheResponseHeader header, string key, byte[] extra, byte[] message)
        {
            if (Callback != null)
                Callback(header.Status);
        }

        public void Fail()
        {
            if (Callback != null)
                Callback(Status.InternalError);
        }

        public override string ToString()
        {
            return Opcode.StartAuth.ToString() + ";Id=" + RequestId + ";Key=" + Key;
        }
    }
}
