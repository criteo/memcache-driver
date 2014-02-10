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
using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.Authenticators
{
    public interface IAuthenticationToken
    {
        /// <summary>
        /// This method returns the authentication status and the next request for authentication
        /// </summary>
        /// <param name="stepRequest">The next request to send for the authentication process</param>
        /// <returns>
        /// NoError when authentication is done
        /// StepRequired when a next step must be sent
        /// AuthRequired when the authentication has failed
        /// </returns>
        Status StepAuthenticate(out IMemcacheRequest stepRequest);
    }
}
