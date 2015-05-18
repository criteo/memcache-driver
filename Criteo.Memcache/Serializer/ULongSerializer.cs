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
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Serializer
{
    class ULongSerializer : ISerializer<ulong>
    {
        public byte[] ToBytes(ulong value)
        {
            var serialized = new byte[4];
            serialized.CopyFrom(0, value);
            return serialized;
        }

        public ulong FromBytes(byte[] value)
        {
            return value == null ? 0 : value.CopyToULong(0);
        }

        public uint TypeFlag
        {
            get { throw new NotImplementedException(); }
        }
    }
}
