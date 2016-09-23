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

using System.Threading;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    /// <summary>
    /// Base class for all requests that can be sent concurrently to several nodes, and for which the user expects only one callback.
    /// </summary>
    abstract class RedundantRequest : MemcacheRequestBase
    {
        private int _receivedResponses = 0;         // Either a fail-to-send, a response from the node, or a transport fail.
        private int _ignoreNextResponses = 0;       // Integer used as a boolean: 0 --> false, !=0 --> true
        private int _aggregatedStatus = (int)Status.KeyNotFound;

        private CallBackPolicy _callBackPolicy;

        protected Status GetResult()
        {
            return (Status)_aggregatedStatus;
        }

        protected RedundantRequest(CallBackPolicy callBackPolicy)
        {
            _callBackPolicy = callBackPolicy;
            switch (_callBackPolicy)
            {
                case CallBackPolicy.AnyOK:
                    _aggregatedStatus = (int)Status.InternalError;
                    break;

                case CallBackPolicy.AllOK:
                    _aggregatedStatus = (int)Status.NoError;
                    break;

                default:
                    _aggregatedStatus = (int)Status.InternalError;
                    break;
            }
        }

        /// <summary>
        /// Aggregation function for returned Statuses.
        /// 
        /// For AnyOK callback policy the logic is:
        /// * if all responses have statuses InternalError then aggregated status is InternalError,
        ///     in other case we was able to send at least one request, so we assume the cluster is OK, and
        /// * if we received at least one NoError then aggregated status is NoError, in other cases
        /// * if we received some error different from KeyNotFound and InternalError return this error as aggregated 
        ///     (we have some problems with connections/auth/network etc. so we want to see this problem)
        /// * return KeyNotFound (here we only have at least one KeyNotFound and maybe several InternalErrors)
        /// 
        /// For AllOK callback policy the logic is:
        /// * if all responses have statuses NoError then aggregated status is NoError
        /// * in any other cases aggregated status if first received error
        /// </summary>
        /// <param name="aggregatedStatus"></param>
        /// <param name="newStatus"></param>
        /// <returns>new aggregated Status of newStatus and aggregatedStatus</returns>
        private Status AggregateStatus(Status aggregatedStatus, Status newStatus)
        {
            switch (_callBackPolicy)
            {
                case CallBackPolicy.AnyOK:
                    if (newStatus == Status.InternalError)
                        return aggregatedStatus; //ignore InternalError
                    if (aggregatedStatus == Status.NoError)
                        return Status.NoError; // we already received NoError, ignore all other statuses
                    if (newStatus != Status.KeyNotFound)
                        return newStatus;
                    if (aggregatedStatus == Status.InternalError) // here we are sure newStatus == KeyNotFound
                        return Status.KeyNotFound;
                    return aggregatedStatus;

                case CallBackPolicy.AllOK:
                    if (aggregatedStatus == Status.NoError)
                        return newStatus;
                    return aggregatedStatus;

                default:
                    return newStatus;
            }
        }
        /// <summary>
        /// This function will return true if the callback associated with the request must be called, and
        /// false otherwise. It must be called on receiving a response from a node, or failing to receive it.
        /// </summary>
        /// <param name="respStatus"></param>
        /// <returns></returns>
        protected bool CallCallback(Status respStatus)
        {
            int receivedResponses = Interlocked.Increment(ref _receivedResponses);

            if (_ignoreNextResponses != 0)
            {
                return false;
            }

            int originalValue;
            int newValue;
            do
            {
                originalValue = _aggregatedStatus;
                newValue = (int)AggregateStatus((Status)originalValue, respStatus);
            } while (Interlocked.CompareExchange(ref _aggregatedStatus, newValue, originalValue) != originalValue);

            // When the last answer is received we must call the callback.
            if (receivedResponses >= Replicas + 1 &&
                0 == Interlocked.CompareExchange(ref _ignoreNextResponses, 1, 0))   // If _ignoreNextResponses was 0 (false), then switch it to 1 (true) and return true.
            {
                return true;
            }

            // Otherwise the condition to call the callback depends on the callback policy
            bool ignoreNextResponses;
            switch (_callBackPolicy)
            {
                case CallBackPolicy.AnyOK:
                    ignoreNextResponses = (respStatus == Status.NoError);
                    break;
                case CallBackPolicy.AllOK:
                    ignoreNextResponses = (respStatus != Status.NoError);
                    break;
                default:
                    ignoreNextResponses = false;
                    break;
            }

            if (ignoreNextResponses &&
                0 == Interlocked.CompareExchange(ref _ignoreNextResponses, 1, 0))   // If _ignoreNextResponses was 0 (false), then switch it to 1 (true) and return true.
            {
                return true;
            }

            return false;
        }
    }
}
