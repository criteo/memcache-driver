using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;
using System.Threading;


namespace Criteo.Memcache.Requests
{
    /// <summary>
    /// Base class for all requests that can be sent concurrently to several nodes, and for which the user expects only one callback.     
    /// </summary>
    class RedundantRequest
    {
        private int _receivedAnswers = 0;
        private bool _ignoreNextResponses = false;

        public CallBackPolicy CallBackPolicy { get; set; }
        public int Replicas { get; private set; }

        public RedundantRequest(int replicas)
        {
            Replicas = replicas;
        }

        /// <summary>
        /// This function should be called after the last request has been sent to the nodes. It indicates the total
        /// nummber of requests actually sent. In the (rare) event when sentRequests is smaller than Replicas+1, and
        /// HandleReponse() has already been called for all sent requests, this function will call the callback (if
        /// not already done) with an InternalError status, to finalize the Command.
        /// </summary>
        /// <param name="sentRequests">nummber of requests actually sent</param>
        /// <returns></returns>
        protected bool CallCallbackOnLastSent(int sentRequests)
        {
            if (sentRequests >= 1)
            {
                lock (this)
                {
                    if (sentRequests < Replicas + 1 && !_ignoreNextResponses)
                    {
                        _ignoreNextResponses = true;
                        return true;
                    }
                }
            }
            return false;
        }

        protected bool CallCallback(ref Status resp_status)
        {
            lock (this)
            {
                _receivedAnswers++;
                if (_ignoreNextResponses)
                {
                    return false;
                }
                if (_receivedAnswers >= Replicas + 1)
                {
                    // Last answer has been received. We must call the callback.
                    _ignoreNextResponses = true;
                    return true;
                }
                switch (CallBackPolicy)
                {
                    case CallBackPolicy.AnyOK:
                        _ignoreNextResponses = (resp_status == Status.NoError);
                        break;
                    case CallBackPolicy.AllOK:
                        _ignoreNextResponses = (resp_status != Status.NoError);
                        break;
                    default:
                        break;
                }
                return _ignoreNextResponses;
            }
        }
    }
}
