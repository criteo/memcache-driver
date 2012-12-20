using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Node;
using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Sockets
{
    internal abstract class MemcacheSocketBase : IMemcacheSocket
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
        public IMemcacheNodeQueue WaitingRequests { protected get; set; }
        protected ConcurrentQueue<IMemcacheRequest> PendingRequests { get; private set; }
        private IMemcacheAuthenticator _authenticator;
        protected IAuthenticationToken AuthenticationToken { get; set; }

        internal MemcacheSocketBase(EndPoint endpoint, IMemcacheNodeQueue itemQueue, IMemcacheAuthenticator authenticator)
        {
            EndPoint = endpoint;
            WaitingRequests = itemQueue;
            PendingRequests = new ConcurrentQueue<IMemcacheRequest>();
            _authenticator = authenticator;

            Reset();
        }

        protected abstract void ShutDown();
        protected abstract void Start();

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
            var oldPending = PendingRequests;

            // restart the while thing
            PendingRequests = new ConcurrentQueue<IMemcacheRequest>();
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
                        _startAttemptTimer.Change(1000, Timeout.Infinite);
                    }
                }, null, 0, Timeout.Infinite);

            // take the needed time to resend the aborted requests
            IMemcacheRequest item;
            while (oldPending.Count > 0)
                if (oldPending.TryDequeue(out item))
                    WaitingRequests.Add(item);
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
                PendingRequests.TryDequeue(out result);
                if (result.RequestId != header.Opaque)
                    throw new Exception("Received a response that doesn't match with the sent request queue");
            }

            return result;
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
                request = WaitingRequests.Take();

            return request;
        }


        public void Dispose()
        {
            if (Socket != null)
                Socket.Dispose();
        }
    }
}
