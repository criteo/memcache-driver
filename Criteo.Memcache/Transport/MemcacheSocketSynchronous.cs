using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.Memcache.Node;
using Criteo.Memcache.Requests;
using Criteo.Memcache.Authenticators;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Transport
{
    internal class MemcacheSocketSynchronous : MemcacheSocketBase, IMemcacheTransport
    {
        private CancellationTokenSource _token;
        private ConcurrentQueue<IMemcacheRequest> _pending = new ConcurrentQueue<IMemcacheRequest>();
        private volatile Action<MemcacheSocketSynchronous> _setupAction;
        private bool _threadedReceived = false;

        public MemcacheSocketSynchronous(EndPoint endpoint, IMemcacheAuthenticator authenticator, IMemcacheNode node, int queueTimeout, int pendingLimit, Action<MemcacheSocketSynchronous> setupAction, bool threaded)
            : base(endpoint, authenticator, queueTimeout, pendingLimit, null, node)
        {
            _setupAction = setupAction;
            _threadedReceived = threaded;

            Reset(null);
        }

        #region Threaded Reads
        private Thread _receivingThread;

        private void StartReceivingThread()
        {
            var buffer = new byte[MemcacheResponseHeader.SIZE];
            _receivingThread = new Thread(t =>
            {
                var token = (CancellationToken)t;
                while (!token.IsCancellationRequested)
                {
                    var socket = Socket;
                    try
                    {
                        int received = 0;
                        do
                        {
                            received += socket.Receive(buffer, received, MemcacheResponseHeader.SIZE - received, SocketFlags.None);
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
                                received += socket.Receive(extra, received, header.ExtraLength - received, SocketFlags.None);
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
                                received += socket.Receive(message, received, messageLength - received, SocketFlags.None);
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
                        if (!token.IsCancellationRequested && !Disposed)
                        {
                            if (_transportError != null)
                                _transportError(e);

                            Reset(socket);
                        }
                    }
                }
            });
            _receivingThread.Start(_token.Token);
        }
        #endregion Threaded Reads

        #region Async Reads
        private SocketAsyncEventArgs _receiveArgs;

        private void InitReadResponse()
        {
            _receiveArgs = new SocketAsyncEventArgs();
            _receiveArgs.SetBuffer(new byte[MemcacheResponseHeader.SIZE], 0, MemcacheResponseHeader.SIZE);
            _receiveArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnReadResponseComplete);
        }

        private void ReadResponse()
        {
            var socket = Socket;
            try
            {
                _receiveArgs.SetBuffer(0, MemcacheResponseHeader.SIZE);
                if (!socket.ReceiveAsync(_receiveArgs))
                    OnReadResponseComplete(socket, _receiveArgs);
            }
            catch (Exception e)
            {
                if (!Disposed)
                {
                    if (_transportError != null)
                        _transportError(e);
                    Reset(socket);
                }
            }
        }

        private void OnReadResponseComplete(object sender, SocketAsyncEventArgs args)
        {
            var socket = sender as Socket;
            try
            {
                // if the socket has been disposed, don't raise an error
                if (args.SocketError == SocketError.OperationAborted)
                    return;
                if (args.SocketError != SocketError.Success)
                    throw new SocketException((int)args.SocketError);

                // check if we read a full header, else continue
                if (args.BytesTransferred + args.Offset < MemcacheResponseHeader.SIZE)
                {
                    int offset = args.BytesTransferred + args.Offset;
                    args.SetBuffer(offset, MemcacheResponseHeader.SIZE - offset);
                    if (!socket.ReceiveAsync(args))
                        OnReadResponseComplete(socket, args);
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

                if (_memcacheResponse != null)
                    _memcacheResponse(header, request);

                if (header.Status != Status.NoError && _memcacheError != null)
                    _memcacheError(header, request);

                // loop the read on the socket
                ReadResponse();
            }
            catch (Exception e)
            {
                if (!Disposed)
                {
                    if (_transportError != null)
                        _transportError(e);
                    Reset(socket);
                }
            }
        }
        #endregion Async Reads

        protected override void Start()
        {
            _token = new CancellationTokenSource();

            if (_threadedReceived)
                StartReceivingThread();
            else
            {
                InitReadResponse();
                ReadResponse();
            }

            Authenticate();
        }

        protected override void ShutDown()
        {
            try
            {
                if (_token != null)
                    _token.Cancel();
            }
            catch (AggregateException e)
            {
                _transportError(e);
            }

            var socket = Socket;
            if (socket != null)
                socket.Dispose();

            if (_receiveArgs != null)
                _receiveArgs.Dispose();
        }

        private bool Authenticate()
        {
            bool authDone = false;
            IMemcacheRequest request = null;
            Status authStatus = Status.NoError;
            if (AuthenticationToken != null)
            {
                while (!authDone)
                {
                    authStatus = AuthenticationToken.StepAuthenticate(out request);

                    switch (authStatus)
                    {
                        // auth OK, clear the token
                        case Status.NoError:
                            AuthenticationToken = null;
                            authDone = true;
                            break;
                        case Status.StepRequired:
                            if (request == null)
                            {
                                if (_transportError != null)
                                    _transportError(new AuthenticationException("Unable to authenticate : step required but no request from token"));
                                Reset(Socket);
                                return false;
                            }
                            return SendRequest(request);
                        default:
                            if (_transportError != null)
                                _transportError(new AuthenticationException("Unable to authenticate : status " + authStatus.ToString()));
                            Reset(Socket);
                            return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Synchronously sends the request
        /// </summary>
        /// <param name="request"></param>
        public override bool TrySend(IMemcacheRequest request)
        {
            if (request == null)
                return false;

            if (!Initialized && !Initialize())
                return false;

            return SendRequest(request);
        }

        private Timer _reconnectTimer;
        private void PlanReconnect()
        {
            _reconnectTimer = new Timer(_ =>
                {
                    bool reconnected = false;
                    try
                    {
                        reconnected = Initialize();
                    }
                    catch (Exception e)
                    {
                        if (_transportError != null)
                            _transportError(e);
                        reconnected = false;
                    }

                    if (reconnected)
                    {
                        _reconnectTimer.Dispose();
                        if (_setupAction != null)
                            _setupAction(this);
                    }
                    else
                    {
                        _reconnectTimer.Change(1000, Timeout.Infinite);
                    }
                }, null, 1000, Timeout.Infinite);
        }

        private bool SendRequest(IMemcacheRequest request)
        {
            var socket = Socket;
            try
            {
                var buffer = request.GetQueryBuffer();

                if (!EnqueueRequest(request, _token.Token))
                    return false;

                int sent = 0;
                do
                {
                    sent += socket.Send(buffer, sent, buffer.Length - sent, SocketFlags.None);
                } while (sent != buffer.Length);
                return true;
            }
            catch (Exception e)
            {
                if (_transportError != null)
                    _transportError(e);

                Reset(socket);

                return false;
            }
        }

        public override void PlanSetup()
        {
            if (Initialized && _setupAction != null)
                // the transport is already up => execute now
                _setupAction(this);
        }
    }
}
