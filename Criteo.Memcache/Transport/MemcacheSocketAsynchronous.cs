using System;
using System.Collections.Concurrent;
using System.Net;

using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Transport
{
    abstract class MemcacheSocketAsynchronous : MemcacheSocketBase
    {
        private IMemcacheRequestsQueue _requestsQueue;
        public override IMemcacheRequestsQueue RequestsQueue { get { return _requestsQueue; } }

        public MemcacheSocketAsynchronous(EndPoint endpoint, IMemcacheAuthenticator authenticator, IMemcacheRequestsQueue itemQueue)
            : base(endpoint, authenticator)
        {
            _requestsQueue = itemQueue;
        }

        protected override void DisposePending(ConcurrentQueue<IMemcacheRequest> pending)
        {
            base.Reset();

            // take the needed time to resend the aborted requests
            IMemcacheRequest item;
            while (pending.Count > 0)
                if (pending.TryDequeue(out item))
                    RequestsQueue.Add(item);
        }

        protected IMemcacheRequest GetNextRequest()
        {
            IMemcacheRequest request = null;
            Status authStatus = Status.NoError;
            if (AuthenticationToken != null)
            {
                authStatus = AuthenticationToken.StepAuthenticate(out request);

                switch (authStatus)
                {
                    // auth OK, clear the token
                    case Status.NoError:
                        AuthenticationToken = null;
                        break;
                    case Status.StepRequired:
                        if (request == null && _transportError != null)
                        {
                            _transportError(new AuthenticationException("Unable to authenticate : step required but no request from token"));
                            Reset();
                            return null;
                        }
                        break;
                    default:
                        if (_transportError != null)
                            _transportError(new AuthenticationException("Unable to authenticate : status " + authStatus.ToString()));
                        Reset();
                        return null;
                }
            }

            if (authStatus == Status.NoError)
                request = RequestsQueue.Take();

            return request;
        }
    }
}
