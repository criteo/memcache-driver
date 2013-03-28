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
        #region Events
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
        #endregion Event

        private RequestQueue _pendingRequests;
        private IMemcacheAuthenticator _authenticator;
        private IMemcacheNode _node;

        protected EndPoint EndPoint { get; private set; }
        protected Socket Socket { get; set; }
        protected IAuthenticationToken AuthenticationToken { get; set; }
        protected bool Disposed { get; private set; }
        protected bool Initialized { get; set; }

        private int _requestLimit;
        private int _queueTimeout;

        internal MemcacheSocketBase(EndPoint endpoint, IMemcacheAuthenticator authenticator, int queueTimeout, int pendingLimit, IMemcacheRequestsQueue queue, IMemcacheNode node)
        {
            Disposed = false;
            Initialized = false;
            EndPoint = endpoint;
            _authenticator = authenticator;
            _requestLimit = 1000;
            _queueTimeout = Timeout.Infinite;
            RequestsQueue = queue;
            _node = node;
        }

        protected abstract void ShutDown();
        protected abstract void Start();
        public IMemcacheRequestsQueue RequestsQueue { get; private set; }

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
        protected void Reset(Socket socketFailing)
        {
            RequestQueue oldPending = null;

            lock (this)
            {
                if (Disposed ||
                    (Socket != null && !object.ReferenceEquals(socketFailing, Socket)))
                    return;

                // somthing goes wrong, stop to send
                ShutDown();
                Initialized = false;

                // keep the pending request somewhere
                oldPending = Interlocked.Exchange(ref _pendingRequests, new RequestQueue(_queueTimeout, _requestLimit));
            }

            if (oldPending != null)
            {
                foreach (var request in oldPending.Requests)
                    if (_node != null && !_node.TrySend(request, -1))
                        request.Fail();
                oldPending.Dispose();
            }
        }

        protected bool Initialize()
        {
            try
            {
                lock (this)
                {
                    if (!Initialized && !Disposed)
                    {
                        CreateSocket();
                        Start();

                        Initialized = true;
                    }
                }
            }
            catch (Exception e2)
            {
                if (_transportError != null)
                    _transportError(e2);
            }

            return Initialized;
        }

        protected IMemcacheRequest UnstackToMatch(MemcacheResponseHeader header)
        {
            IMemcacheRequest result = null;

            if (header.Opcode.IsQuiet())
            {
                throw new MemcacheException("No way we can match a quiet request !");
            }
            else
            {
                if (!_pendingRequests.TryDequeue(out result))
                    throw new MemcacheException("Received a response when no request is pending");
                if (result.RequestId != header.Opaque)
                    throw new MemcacheException("Received a response that doesn't match with the sent request queue");
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

        public virtual void Dispose()
        {
            // block the start of any resets, then shut down
            lock (this)
            {
                Disposed = true;
                int attempt = 0;

                _startAttemptTimer = new Timer(socket =>
                {
                    try
                    {
                        lock (this)
                        {
                            if (_pendingRequests.IsEmpty || ++attempt > 5)
                            {
                                try
                                {
                                    ShutDown();
                                }
                                finally
                                {
                                    // fail of pending requests
                                    if (_pendingRequests != null)
                                    {
                                        foreach (var request in _pendingRequests.Requests)
                                            request.Fail();

                                        _pendingRequests.Dispose();
                                        _pendingRequests = null;
                                    }
                                    _startAttemptTimer.Dispose();
                                    _startAttemptTimer = null;
                                }
                            }
                        }
                    }
                    catch (Exception e2)
                    {
                        if (_transportError != null)
                            _transportError(e2);
                    }
                }, null, 1000, 1000);

            }
        }

        private class RequestQueue : IDisposable
        {
            private ConcurrentQueue<IMemcacheRequest> _pendingRequests;
            private SemaphoreSlim _requestLimiter = null;
            private int _timeout;

            public bool IsEmpty
            {
                get
                {
                    return _pendingRequests.IsEmpty;
                }
            }

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
