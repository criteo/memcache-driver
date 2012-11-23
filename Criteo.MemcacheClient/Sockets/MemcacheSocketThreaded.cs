using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.Sockets
{
    public class MemcacheSocketThreaded : MemcacheSocketBase
    {
        private Thread _sendingThread;
        private Thread _receivingThread;

        private CancellationTokenSource _token;

        public MemcacheSocketThreaded(IPEndPoint endPoint, BlockingCollection<IMemcacheRequest> itemQueue)
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
            var buffer = new byte[24];
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
                            received += Socket.Receive(buffer, received, 24 - received, SocketFlags.None);
                        } while (received < 24);

                        var header = new MemacheResponseHeader();
                        header.FromData(buffer);

                        byte[] message = null;
                        // in case we have a message ! (should not happen for a set)
                        if (header.TotalBodyLength > 0)
                        {
                            message = new byte[header.TotalBodyLength];
                            Socket.Receive(message);
                        }

                        // should assert we have the good request
                        var request = UnstackToMatch(header);

                        if (_memcacheResponse != null)
                            _memcacheResponse(header, request);
                        if (header.Status != Status.NoError && _memcacheError != null)
                            _memcacheError(header, request);

                        if (request != null)
                            request.HandleResponse(header, message);
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
            _receivingThread.Start(_token);
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
