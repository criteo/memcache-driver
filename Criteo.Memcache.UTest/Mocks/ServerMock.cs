using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;

using Criteo.Memcache.Headers;
using System.Threading;

namespace Criteo.Memcache.UTest.Mocks
{
    class ServerMock : IDisposable
    {
        private Socket _socket;
        private List<Socket> _acceptedSockets;

        private bool _disposed = false;

        public MemcacheRequestHeader LastReceivedHeader { get; private set; }
        public byte[] LastReceivedBody { get; set; }

        public byte[] ResponseHeader { get; private set; }
        public byte[] ResponseBody { private get; set; }
        public ManualResetEventSlim ReceiveMutex { get; set; }
        public EndPoint ListenEndPoint { get; private set; }

        public int MaxSent { get; set; }

        /// <summary>
        /// Start and listen ongoing TCP connections
        /// </summary>
        public ServerMock()
        {
            var endpoint = new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 0);
            _socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Bind(endpoint);
            ListenEndPoint = _socket.LocalEndPoint;
            _socket.Listen((int)SocketOptionName.MaxConnections);
            var acceptEventArgs = GetAcceptEventArgs();
            if (!_socket.AcceptAsync(acceptEventArgs))
                throw new Exception("Unable to listen on " + ListenEndPoint.ToString());
            _acceptedSockets = new List<Socket>();
            ResponseHeader = new byte[MemcacheResponseHeader.SIZE];
        }

        private SocketAsyncEventArgs GetAcceptEventArgs()
        {
            var acceptEventArgs = new SocketAsyncEventArgs();
            acceptEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnAccept);
            return acceptEventArgs;
        }

        /// <summary>
        /// When on incoming connection appear start receiving headers on it
        /// </summary>
        /// <param name="sender" />
        /// <param name="e" />
        void OnAccept(object sender, SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
                return;

            var socket = sender as Socket;
            var acceptedSocket = e.AcceptSocket;
            _acceptedSockets.Add(acceptedSocket);
            var eventArg = new SocketAsyncEventArgs();
            eventArg.SetBuffer(new byte[MemcacheRequestHeader.SIZE], 0, MemcacheRequestHeader.SIZE);
            eventArg.Completed += new EventHandler<SocketAsyncEventArgs>(OnReceive);
            acceptedSocket.ReceiveAsync(eventArg);

            var acceptEventArgs = GetAcceptEventArgs();
            if (!_socket.AcceptAsync(acceptEventArgs))
                throw new Exception("Unable to accept further connections");
        }

        /// <summary>
        /// A header is received =>
        /// * Read the body if present
        /// * Send the response header
        /// * Send the body if present
        /// * Start receiving again
        /// </summary>
        /// <param name="sender" />
        /// <param name="e" />
        void OnReceive(object sender, SocketAsyncEventArgs e)
        {
            if (ReceiveMutex != null)
                ReceiveMutex.Wait();

            // ends when error occur
            if (e.SocketError != SocketError.Success)
                return;

            // ends request header transfer
            var socket = sender as Socket;
            int transfered = e.BytesTransferred;
            while (transfered < MemcacheRequestHeader.SIZE && !_disposed)
                transfered += socket.Receive(e.Buffer, transfered, MemcacheRequestHeader.SIZE - transfered, SocketFlags.None);

            // read the request header
            var header = new MemcacheRequestHeader();
            header.FromData(e.Buffer);
            LastReceivedHeader = header;

            // transfer the body is present
            if (header.TotalBodyLength > 0)
            {
                var body = new byte[header.TotalBodyLength];
                transfered = 0;

                while (transfered < header.TotalBodyLength && !_disposed)
                    transfered += socket.Receive(body, transfered, (int)header.TotalBodyLength - transfered, SocketFlags.None);

                LastReceivedBody = body;
            }
            else
                LastReceivedBody = null;

            // send the response header
            transfered = 0;
            while (transfered < MemcacheResponseHeader.SIZE && !_disposed)
            {
                var toTransfer = MemcacheResponseHeader.SIZE - transfered;
                if (MaxSent != 0 && MaxSent < toTransfer)
                    toTransfer = MaxSent;
                transfered += socket.Send(ResponseHeader, transfered, toTransfer, SocketFlags.None);
            }

            // send the response body if present
            if (ResponseBody != null)
            {
                transfered = 0;
                while (transfered < ResponseBody.Length && !_disposed)
                {
                    var toTransfer = ResponseBody.Length - transfered;
                    if (MaxSent != 0 && MaxSent < toTransfer)
                        toTransfer = MaxSent;
                    transfered += socket.Send(ResponseBody, transfered, toTransfer, SocketFlags.None);
                }
            }

            // start to receive again
            if (!_disposed)
                socket.ReceiveAsync(e);
        }

        public void Dispose()
        {
            _disposed = true;
            if(_socket != null)
                _socket.Dispose();
            foreach (var socket in _acceptedSockets)
            {
                socket.Dispose();
            }
        }
    }
}
