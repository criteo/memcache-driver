using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.MemcacheClient.Requests;
using Criteo.MemcacheClient.Headers;
using Criteo.MemcacheClient.Node;

namespace Criteo.MemcacheClient.Sockets
{
    internal class MemcacheSocketThreaded : MemcacheSocketBase
    {
        private Thread _sendingThread;
        private Thread _receivingThread;

        private CancellationTokenSource _token;

        public MemcacheSocketThreaded(IPEndPoint endPoint, IMemcacheNodeQueue itemQueue)
            : base(endPoint, itemQueue)
        {
        }

        private void StartSendingThread()
        {
            _sendingThread = new Thread(t =>
            {
                var token = (CancellationToken)t;
                uint requestId = 0;
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        var request = WaitingRequests.Take();
                        request.RequestId = ++requestId;
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
                            Socket.Receive(extra);
                        }
                        byte[] message = null;
                        if (header.TotalBodyLength - header.ExtraLength > 0)
                        {
                            message = new byte[header.TotalBodyLength - header.ExtraLength];
                            Socket.Receive(message);
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

                            _receivingThread = null;
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
            _token.Cancel();
            Socket.Dispose();
        }
    }
}
