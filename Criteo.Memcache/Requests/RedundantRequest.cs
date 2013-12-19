using System.Threading;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    /// <summary>
    /// Base class for all requests that can be sent concurrently to several nodes, and for which the user expects only one callback.
    /// </summary>
    class RedundantRequest
    {
        private int _receivedResponses = 0;         // Either a fail-to-send, a response from the node, or a transport fail.
        private int _ignoreNextResponses = 0;       // Integer used as a boolean: 0 --> false, !=0 --> true

        public CallBackPolicy CallBackPolicy { get; set; }
        public int Replicas { get; set; }

        /// <summary>
        /// This function will return true if the callback associated with the request must be called, and
        /// false otherwise. It must be called on receiving a response from a node, or failing to receive it.
        /// </summary>
        /// <param name="resp_status"></param>
        /// <returns></returns>
        protected bool CallCallback(Status resp_status)
        {
            int receivedResponses = Interlocked.Increment(ref _receivedResponses);

            if (_ignoreNextResponses != 0)
            {
                return false;
            }

            // When the last answer is received we must call the callback.
            if (receivedResponses >= Replicas + 1 &&
                0 == Interlocked.CompareExchange(ref _ignoreNextResponses, 1, 0))   // If _ignoreNextResponses was 0 (false), then switch it to 1 (true) and return true.
            {
                return true;
            }

            // Otherwise the condition to call the callback depends on the callback policy
            bool ignoreNextResponses;
            switch (CallBackPolicy)
            {
                case CallBackPolicy.AnyOK:
                    ignoreNextResponses = (resp_status == Status.NoError);
                    break;
                case CallBackPolicy.AllOK:
                    ignoreNextResponses = (resp_status != Status.NoError);
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
