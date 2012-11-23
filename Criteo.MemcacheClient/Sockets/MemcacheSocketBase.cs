using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.Sockets
{
    public abstract class MemcacheSocketBase : IMemcacheSocket
    {
        private BlockingCollection<IMemcacheRequest> _waitingRequests;

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

        protected Action<MemacheResponseHeader, IMemcacheRequest> _memcacheError;
        public event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheError
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

        protected Action<MemacheResponseHeader, IMemcacheRequest> _memcacheResponse;
        public event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheResponse
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
        protected BlockingCollection<IMemcacheRequest> WaitingRequests { get { return _waitingRequests; } }
        protected ConcurrentQueue<IMemcacheRequest> PendingRequests { get; private set; }

        public MemcacheSocketBase(EndPoint endPoint, int mtu = 1450)
            : this(endPoint, new BlockingCollection<IMemcacheRequest>())
        {
        }

        internal MemcacheSocketBase(EndPoint endpoint, BlockingCollection<IMemcacheRequest> itemQueue)
        {
            EndPoint = endpoint;
            _waitingRequests = itemQueue;
            PendingRequests = new ConcurrentQueue<IMemcacheRequest>();

            CreateSocket();
            Start();
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
                    catch (Exception)
                    {
                        _startAttemptTimer.Change(1000, Timeout.Infinite);
                    }
                }, null, 0, Timeout.Infinite);

            // take the needed time to resend the aborted requests
            IMemcacheRequest item;
            while (oldPending.Count > 0)
                if (oldPending.TryDequeue(out item))
                    _waitingRequests.Add(item);
        }

        protected IMemcacheRequest UnstackToMatch(MemacheResponseHeader header)
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

    }
}
