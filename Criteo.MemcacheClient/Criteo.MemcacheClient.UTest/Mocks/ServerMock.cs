using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;

using Criteo.MemcacheClient.Headers;

namespace Criteo.MemcacheClient.UTest.Mocks
{
    class ServerMock
    {
        Socket _socket;

        public ServerMock(IPEndPoint endPoint)
        {
            _socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Bind(endPoint);
            _socket.Listen((int)SocketOptionName.MaxConnections);
            var eventArg = new SocketAsyncEventArgs();
            eventArg.Completed += new EventHandler<SocketAsyncEventArgs>(OnAccept);
            _socket.AcceptAsync(eventArg);
        }

        void OnAccept(object sender, SocketAsyncEventArgs e)
        {
            var acceptedSocket = e.AcceptSocket;
            var eventArg = new SocketAsyncEventArgs();
            eventArg.SetBuffer(new byte[24], 0, 24);
            eventArg.Completed += new EventHandler<SocketAsyncEventArgs>(OnReceive);
            acceptedSocket.ReceiveAsync(eventArg);

            _socket.AcceptAsync(e);
        }

        void OnReceive(object sender, SocketAsyncEventArgs e)
        {
            var socket = sender as Socket;
            int transfered = e.BytesTransferred;
            while (transfered < 24)
                transfered += socket.Receive(e.Buffer, transfered, 24 - transfered, SocketFlags.None);

            var header = new MemacheRequestHeader();
            header.FromData(e.Buffer);


            //if()
        }
    }
}
