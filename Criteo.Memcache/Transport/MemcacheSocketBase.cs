using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Node;
using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Transport
{
    internal abstract class MemcacheSocketBase : IMemcacheTransport
    {
        protected Action<Exception> _transportError;
        public event Action<Exception> TransportError
        {
            add
            {
                _transportError += value;
            }
            remove
            {
                _transportError -= value;
            }
        }

        protected Action<MemcacheResponseHeader, IMemcacheRequest> _memcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError
        {
            add
            {
                _memcacheError += value;
            }
            remove
            {
                _memcacheError -= value;
            }
        }

        protected Action<MemcacheResponseHeader, IMemcacheRequest> _memcacheResponse;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse
        {
            add
            {
                _memcacheResponse += value;
            }
            remove
            {
                _memcacheResponse -= value;
            }
        }

        protected EndPoint EndPoint { get; private set; }
        protected Socket Socket { get; set; }
        private RequestQueue _pendingRequests;
        private IMemcacheAuthenticator _authenticator;
        protected IAuthenticationToken AuthenticationToken { get; set; }

        private int _requestLimit;
        private int _queueTimeout;

        internal MemcacheSocketBase(EndPoint endpoint, IMemcacheAuthenticator authenticator, int queueTimeout, int pendingLimit)
        {
            EndPoint = endpoint;
            _authenticator = authenticator;
            _requestLimit = 1000;
            _queueTimeout = Timeout.Infinite;

            Reset();
        }

        protected abstract void ShutDown();
        protected abstract void Start();
        public abstract IMemcacheRequestsQueue RequestsQueue { get; }

        protected void CreateSocket()
        {
            var socket = new Socket(EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(EndPoint);
            socket.ReceiveBufferSize = (2 << 15);
            socket.SendBufferSize = (2 << 15);

            if (Socket != null)
                Socket.Dispose();
            Socket = socket;

            if (_authenticator != null)
                AuthenticationToken = _authenticator.CreateToken();
        }

        private Timer _startAttemptTimer;
        protected void Reset()
        {
            // somthing goes wrong, stop to send
            ShutDown();

            // keep the pending request somewhere
            var oldPending = Interlocked.Exchange(ref _pendingRequests, new RequestQueue(_queueTimeout, _requestLimit));

            // restart the while thing
            _startAttemptTimer = new Timer(_ =>
                {
                    try
                    {
                        CreateSocket();
                        Start();
                    }
                    catch (Exception e)
                    {
                        if (_transportError != null)
                            _transportError(e);
                        _startAttemptTimer.Change(10000, Timeout.Infinite);
                    }
                }, null, 0, Timeout.Infinite);


            if (oldPending != null)
            {
                foreach (var item in oldPending.Requests)
                    RequestsQueue.Add(item);
                oldPending.Dispose();
            }
        }

        protected IMemcacheRequest UnstackToMatch(MemcacheResponseHeader header)
        {
            IMemcacheRequest result = null;

            if (header.Opcode.IsQuiet())
            {
                throw new Exception("No way we can match a quiet request !");
            }
            else
            {
                if (!_pendingRequests.TryDequeue(out result))
                    throw new Exception("Received a response when no request is pending");
                if (result.RequestId != header.Opaque)
                    throw new Exception("Received a response that doesn't match with the sent request queue");
            }

            return result;
        }

        protected bool EnqueueRequest(IMemcacheRequest request)
        {
            return _pendingRequests.EnqueueRequest(request);
        }

        protected bool EnqueueRequest(IMemcacheRequest request, CancellationToken token)
        {
            return _pendingRequests.EnqueueRequest(request, token);
        }

        public void Dispose()
        {
            if (Socket != null)
                Socket.Dispose();
            if (_pendingRequests != null)
                _pendingRequests.Dispose();
        }

        private class RequestQueue : IDisposable
        {
            private ConcurrentQueue<IMemcacheRequest> _pendingRequests;
            private SemaphoreSlim _requestLimiter = null;
            private int _timeout;

            public RequestQueue(int timeout, int limit)
            {
                _timeout = timeout;
                _pendingRequests = new ConcurrentQueue<IMemcacheRequest>();
                if (limit > 0)
                    _requestLimiter = new SemaphoreSlim(limit);
            }

            public bool EnqueueRequest(IMemcacheRequest request)
            {
                if (!_requestLimiter.Wait(_timeout))
                    return false;

                _pendingRequests.Enqueue(request);
                return true;
            }

            public bool EnqueueRequest(IMemcacheRequest request, CancellationToken token)
            {
                if(!_requestLimiter.Wait(_timeout, token))
                    return false;

                _pendingRequests.Enqueue(request);
                return true;
            }

            public bool TryDequeue(out IMemcacheRequest request)
            {
                if (_pendingRequests.TryDequeue(out request))
                { 
                    _requestLimiter.Release();
                    return true;
                }
                return false;
            }

            /// <summary>
            /// Not thread-safe shouldn't be used while the object is shared
            /// </summary>
            /// <returns></returns>
            public IEnumerable<IMemcacheRequest> Requests
            {
                get
                {
                    IMemcacheRequest request;
                    while (_pendingRequests.Count > 0)
                        if (_pendingRequests.TryDequeue(out request))
                            yield return request;
                }
            }

            public void Dispose()
            {
                if (_requestLimiter != null)
                    _requestLimiter.Dispose();
            }
        }
    }
}
