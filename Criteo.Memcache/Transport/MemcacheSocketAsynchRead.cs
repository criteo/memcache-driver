using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;
using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Transport
{
    internal class MemcacheSocketAsynchRead : MemcacheSocketAsynchronous
    {
        public MemcacheSocketAsynchRead(IPEndPoint endPoint, IMemcacheRequestsQueue queue, IMemcacheNode node, IMemcacheAuthenticator authenticator, int queueTimeout, int pendingLimit)
            : base(endPoint, authenticator, queue, node, queueTimeout, pendingLimit)
        {
        }

        private SocketAsyncEventArgs _receiveArgs;

        private void InitReadResponse()
        {
            _receiveArgs = new SocketAsyncEventArgs();
            _receiveArgs.SetBuffer(new byte[MemcacheResponseHeader.SIZE], 0, MemcacheResponseHeader.SIZE);
            _receiveArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnReadResponseComplete);
        }

        private void ReadResponse()
        {
            _receiveArgs.SetBuffer(0, MemcacheResponseHeader.SIZE);
            Socket.ReceiveAsync(_receiveArgs);
        }

        private void OnReadResponseComplete(object sender, SocketAsyncEventArgs args)
        {
            var socket = sender as Socket;
            try
            {
                // check if we read a full header, else continue
                if (args.BytesTransferred + args.Offset < MemcacheResponseHeader.SIZE)
                {
                    int offset = args.BytesTransferred + args.Offset;
                    args.SetBuffer(offset, MemcacheResponseHeader.SIZE - offset);
                    socket.ReceiveAsync(args);
                    return;
                }

                var header = new MemcacheResponseHeader(args.Buffer);

                byte[] extra = null;
                byte[] message = null;
                int received;
                // in case we have a message ! (should not happen for a set)
                if (header.ExtraLength > 0)
                {
                    extra = new byte[header.ExtraLength];
                    received = 0;
                    do
                    {
                        received += socket.Receive(extra, received, header.ExtraLength - received, SocketFlags.None);
                    } while (received < header.ExtraLength);
                }
                int messageLength = (int)(header.TotalBodyLength - header.ExtraLength);
                if (header.TotalBodyLength - header.ExtraLength > 0)
                {
                    message = new byte[header.TotalBodyLength - header.ExtraLength];
                    received = 0;
                    do
                    {
                        received += socket.Receive(message, received, messageLength - received, SocketFlags.None);
                    } while (received < messageLength);
                }

                // should assert we have the good request
                var request = UnstackToMatch(header);
                if (request != null)
                    request.HandleResponse(header, extra, message);

                if (_memcacheResponse !=  null)
                    _memcacheResponse(header, request);

                // TODO : should I keep that or the request only have to handle it ?
                if (header.Status != Status.NoError && _memcacheError != null)
                    _memcacheError(header, request);

                // loop the read on the socket
                ReadResponse();
            }
            catch (Exception e)
            {
                if (_transportError != null)
                    _transportError(e);
                Reset(socket);
                Initialize();
            }
        }

        protected override void Start()
        {
            base.Start();
            InitReadResponse();
            ReadResponse();
        }

        protected override void ShutDown()
        {
            if(_receiveArgs != null)
                _receiveArgs.Dispose();

            base.ShutDown();
        }
    }
}
