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

namespace Criteo.Memcache.Serializer
{
    /// <summary>
    /// Base interface used to serialise/deserialize values
    /// It is only usefull for having a common interface between all serializers
    /// Don't directly implement that one, implement the generic one instead
    /// </summary>
    public interface ISerializer
    {
        /// <summary>
        /// This flag is used for compatibility with other clients
        /// it is used to set the "flag" metadata in the store requests
        /// </summary>
        uint TypeFlag { get; }
    }

    /// <summary>
    /// Interface used to serialise/deserialize values
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ISerializer<T> : ISerializer
    {
        /// <summary>
        /// Converts a value to a byte array
        /// </summary>
        /// <param name="value />
        /// <returns />
        byte[] ToBytes(T value);

        /// <summary>
        /// Converts a byte array to a typed value
        /// </summary>
        /// <param name="value" />
        /// <returns />
        T FromBytes(byte[] value);
    }
}
