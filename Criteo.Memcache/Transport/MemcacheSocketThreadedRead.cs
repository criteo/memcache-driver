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
    internal class MemcacheSocketThreadedRead : MemcacheSocketAsynchronous
    {
        private Thread _receivingThread;

        public MemcacheSocketThreadedRead(IPEndPoint endPoint, IMemcacheRequestsQueue queue, IMemcacheNode node, IMemcacheAuthenticator authenticator, int queueTimeout, int pendingLimit)
            : base(endPoint, authenticator, queue, node, queueTimeout, pendingLimit)
        {
        }

        private void StartReceivingThread(CancellationTokenSource tokenSource)
        {
            var buffer = new byte[MemcacheResponseHeader.SIZE];
            var token = tokenSource.Token;

            _receivingThread = new Thread(() =>
            {
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
            _receivingThread.Start();
        }

        protected override void Start()
        {
            base.Start();
            StartReceivingThread(_token);
        }
    }
}
