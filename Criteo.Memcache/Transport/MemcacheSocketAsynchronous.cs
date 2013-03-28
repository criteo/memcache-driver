using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Transport
{
    abstract class MemcacheSocketAsynchronous : MemcacheSocketBase
    {
        private Thread _sendingThread;
        protected CancellationTokenSource _token;

        public MemcacheSocketAsynchronous(EndPoint endpoint, IMemcacheAuthenticator authenticator, IMemcacheRequestsQueue itemQueue, IMemcacheNode node, int queueTimeout, int pendingLimit)
            : base(endpoint, authenticator, queueTimeout, pendingLimit, itemQueue, node)
        {
            Reset(null);
            Initialize();
        }

        protected override void Start()
        {
            _token = new CancellationTokenSource();
            StartSendingThread(_token);
        }

        protected override void ShutDown()
        {
            if (_token != null)
                _token.Cancel();
            var socket = Socket;
            if (socket != null)
            {
                socket.Dispose();
                Socket = null;
            }
        }

        protected void StartSendingThread(CancellationTokenSource tokenSource)
        {
            var token = tokenSource.Token;
            if (!Initialized && !Initialize())
                throw new MemcacheException("Unable to establish connection with " + EndPoint);

            _sendingThread = new Thread(() =>
            {
                while (!token.IsCancellationRequested)
                {
                    var socket = Socket;
                    try
                    {
                        var request = GetNextRequest();
                        if (request == null)
                            return;

                        var buffer = request.GetQueryBuffer();

                        var queued = EnqueueRequest(request, token);
                        if (!queued)
                        {
                            // timeouted, how to manage that?
                            continue;
                        }

                        int sent = 0;
                        do
                        {
                            sent += socket.Send(buffer, sent, buffer.Length - sent, SocketFlags.None);
                        } while (sent != buffer.Length);
                    }
                    catch (OperationCanceledException)
                    {
                        // canceled from outside, the error if occurs has already been handled
                    }
                    catch (Exception e)
                    {
                        if (!token.IsCancellationRequested)
                        {
                            if (_transportError != null)
                                _transportError(e);

                            _sendingThread = null;
                            Reset(socket);
                            Initialize();
                        }
                    }
                }
            });
            _sendingThread.Start();
        }

        protected IMemcacheRequest GetNextRequest()
        {
            IMemcacheRequest request = null;
            Status authStatus = Status.NoError;
            if (AuthenticationToken != null)
            {
                authStatus = AuthenticationToken.StepAuthenticate(out request);

                switch (authStatus)
                {
                    // auth OK, clear the token
                    case Status.NoError:
                        AuthenticationToken = null;
                        break;
                    case Status.StepRequired:
                        if (request == null && _transportError != null)
                        {
                            _transportError(new AuthenticationException("Unable to authenticate : step required but no request from token"));
                            Reset(Socket);
                            Initialize();
                            return null;
                        }
                        break;
                    default:
                        if (_transportError != null)
                            _transportError(new AuthenticationException("Unable to authenticate : status " + authStatus.ToString()));
                        Reset(Socket);
                        Initialize();
                        return null;
                }
            }

            if (authStatus == Status.NoError)
                request = RequestsQueue.Take();

            return request;
        }
    }
}
