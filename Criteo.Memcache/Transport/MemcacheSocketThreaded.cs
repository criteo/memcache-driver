using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Node;
using Criteo.Memcache.Authenticators;

namespace Criteo.Memcache.Transport
{
    internal class MemcacheSocketThreaded : MemcacheSocketAsynchronous
    {
        private Thread _sendingThread;
        private Thread _receivingThread;
        private CancellationTokenSource _token;

        public MemcacheSocketThreaded(IPEndPoint endPoint, IMemcacheRequestsQueue itemQueue, IMemcacheAuthenticator authenticator)
            : base(endPoint, authenticator, itemQueue)
        {
        }

        private void StartSendingThread()
        {
            _sendingThread = new Thread(t =>
            {
                var token = (CancellationToken)t;
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        var request = GetNextRequest();
                        if (request == null)
                            return;

                        var buffer = request.GetQueryBuffer();

                        PendingRequests.Enqueue(request);
                        int sent = 0;
                        do
                        {
                            sent += Socket.Send(buffer, sent, buffer.Length - sent, SocketFlags.None);
                        } while (sent != buffer.Length);
                    }
                    catch (Exception e)
                    {
                        if (!token.IsCancellationRequested)
                        {
                            if (_transportError != null)
                                _transportError(e);

                            _sendingThread = null;
                            Reset();
                        }
                    }
                }
            });
            _sendingThread.Start(_token.Token);
        }

        private void StartReceivingThread()
        {
            var buffer = new byte[MemcacheResponseHeader.SIZE];
            _receivingThread = new Thread(t =>
            {
                var token = (CancellationToken)t;
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        int received = 0;
                        do
                        {
                            received += Socket.Receive(buffer, received, MemcacheResponseHeader.SIZE - received, SocketFlags.None);
                        } while (received < MemcacheResponseHeader.SIZE);

                        var header = new MemcacheResponseHeader(buffer);

                        // in case we have a message ! (should not happen for a set)
                        byte[] extra = null;
                        if (header.ExtraLength > 0)
                        {
                            extra = new byte[header.ExtraLength];
                            received = 0;
                            do
                            {
                                received += Socket.Receive(extra, received, header.ExtraLength - received, SocketFlags.None);
                            } while (received < header.ExtraLength);
                        }
                        byte[] message = null;
                        int messageLength = (int)(header.TotalBodyLength - header.ExtraLength);
                        if (messageLength > 0)
                        {
                            message = new byte[messageLength];
                            received = 0;
                            do
                            {
                                received += Socket.Receive(message, received, messageLength - received, SocketFlags.None);
                            } while (received < messageLength);
                        }

                        // should assert we have the good request
                        var request = UnstackToMatch(header);

                        if (_memcacheResponse != null)
                            _memcacheResponse(header, request);
                        if (header.Status != Status.NoError && _memcacheError != null)
                            _memcacheError(header, request);

                        if (request != null)
                            request.HandleResponse(header, extra, message);
                    }
                    catch (Exception e)
                    {
                        if (!token.IsCancellationRequested)
                        {
                            if (_transportError != null)
                                _transportError(e);

                            Reset();
                        }
                    }
                }
            });
            _receivingThread.Start(_token.Token);
        }

        protected override void Start()
        {
            _token = new CancellationTokenSource();
            StartSendingThread();
            StartReceivingThread();
        }

        protected override void ShutDown()
        {
            if(_token != null)
                _token.Cancel();
            if (Socket != null)
            {
                Socket.Dispose();
                Socket = null;
            }
        }
    }
}
