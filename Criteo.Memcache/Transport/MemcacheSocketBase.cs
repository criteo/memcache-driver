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
        protected ConcurrentQueue<IMemcacheRequest> PendingRequests { get; private set; }
        private IMemcacheAuthenticator _authenticator;
        protected IAuthenticationToken AuthenticationToken { get; set; }

        internal MemcacheSocketBase(EndPoint endpoint, IMemcacheAuthenticator authenticator)
        {
            EndPoint = endpoint;
            PendingRequests = new ConcurrentQueue<IMemcacheRequest>();
            _authenticator = authenticator;

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

            DisposePending(oldPending);
        }

        protected abstract void DisposePending(ConcurrentQueue<IMemcacheRequest> pending);

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

        public void Dispose()
        {
            if (Socket != null)
                Socket.Dispose();
        }
    }
}
