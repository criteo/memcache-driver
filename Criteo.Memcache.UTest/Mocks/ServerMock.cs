using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.UTest.Mocks
{
    class ServerMock : IDisposable
    {
        Socket _socket;

        public MemcacheRequestHeader LastReceivedHeader { get; private set; }
        public byte[] LastReceivedBody { get; set; }

        public MemcacheResponseHeader ResponseHeader { private get; set; }
        public byte[] ResponseBody { private get; set; }

        private SocketAsyncEventArgs _acceptEventArgs;

        /// <summary>
        /// Start and listen ongoing TCP connections
        /// </summary>
        /// <param name="endPoint"></param>
        public ServerMock(IPEndPoint endPoint)
        {
            _socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Bind(endPoint);
            _socket.Listen((int)SocketOptionName.MaxConnections);
            _acceptEventArgs = new SocketAsyncEventArgs();
            _acceptEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnAccept);
            _socket.AcceptAsync(_acceptEventArgs);
        }

        /// <summary>
        /// When on incoming connection appear start receiving headers on it
        /// </summary>
        /// <param name="sender" />
        /// <param name="e" />
        void OnAccept(object sender, SocketAsyncEventArgs e)
        {
            var socket = sender as Socket;
            if (e.SocketError != SocketError.Success)
                return;

            var acceptedSocket = e.AcceptSocket;
            var eventArg = new SocketAsyncEventArgs();
            eventArg.SetBuffer(new byte[MemcacheRequestHeader.SIZE], 0, MemcacheRequestHeader.SIZE);
            eventArg.Completed += new EventHandler<SocketAsyncEventArgs>(OnReceive);
            acceptedSocket.ReceiveAsync(eventArg);

            //socket.AcceptAsync(e);
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
            // ends when error occur
            if (e.SocketError != SocketError.Success)
                return;

            // ends request header transfer
            var socket = sender as Socket;
            int transfered = e.BytesTransferred;
            while (transfered < MemcacheRequestHeader.SIZE)
                transfered += socket.Receive(e.Buffer, transfered, MemcacheRequestHeader.SIZE - transfered, SocketFlags.None);

            // read the request header
            var header = new MemcacheRequestHeader();
            header.FromData(e.Buffer);
            LastReceivedHeader = header;

            // transfer the body is present
            if(header.TotalBodyLength > 0)
            {
                var body = new byte[header.TotalBodyLength];
                transfered = 0;

                while (transfered < header.TotalBodyLength)
                    transfered += socket.Receive(body, transfered, (int)header.TotalBodyLength - transfered, SocketFlags.None);

                LastReceivedBody = body;
            }
            else
                LastReceivedBody = null;

            // send the response header
            var headerToSend = new byte[MemcacheResponseHeader.SIZE];
            ResponseHeader.ToData(headerToSend);
            transfered = 0;
            while (transfered < MemcacheResponseHeader.SIZE)
                transfered += socket.Send(headerToSend, transfered, MemcacheResponseHeader.SIZE - transfered, SocketFlags.None);

            // send the response body if present
            if (ResponseBody != null)
            {
                transfered = 0;
                while (transfered < ResponseBody.Length)
                    transfered += socket.Send(ResponseBody, 0, ResponseBody.Length - transfered, SocketFlags.None);
            }

            // start to receive again
            socket.ReceiveAsync(e);
        }

        public void Dispose()
        {
            if(_socket != null)
                _socket.Dispose();
        }
    }
}
