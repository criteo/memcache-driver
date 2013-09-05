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
    internal class MemcacheSocket : IMemcacheTransport
    {
        #region Events
        public event Action<Exception> TransportError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;
        public event Action<IMemcacheTransport> TransportDead;
        #endregion Event

        private readonly int _queueTimeout;
        // TODO : add me in the Conf
        private readonly int _pendingLimit = 1000;
        private readonly int _windowSize = 2 << 15;
        // END TODO
        private readonly EndPoint _endPoint;
        private readonly IMemcacheAuthenticator _authenticator;
        private readonly Action<MemcacheSocket> _setupAction;
        private readonly Timer _connectTimer;

        private volatile bool _disposed = false;
        private volatile bool _initialized = false;
        private BlockingCollection<IMemcacheRequest> _pendingRequests;
        private ConcurrentQueue<IMemcacheRequest> _pendingQueue;
        private Socket _socket;
        private CancellationTokenSource _token;
        private Timer _disposeAttemptTimer;

        private SocketAsyncEventArgs _sendAsycnhEvtArgs;
        private SocketAsyncEventArgs _receiveHeaderAsycnhEvtArgs;
        private SocketAsyncEventArgs _receiveBodyAsycnhEvtArgs;

        public bool IsAlive { get; private set; }

        /// <summary>
        /// Ctor, intialize things ...
        /// </summary>
        /// <param name="endpoint" />
        /// <param name="authenticator">Object that ables to Sasl authenticate the socket</param>
        /// <param name="queueTimeout" />
        /// <param name="pendingLimit" />
        /// <param name="setupAction">Delegate to call when the transport is alive</param>
        /// <param name="threaded">If true use a thread to synchronously receive on the socket else use the asynchronous API</param>
        public MemcacheSocket(EndPoint endpoint, IMemcacheAuthenticator authenticator, int queueTimeout, int pendingLimit, Action<MemcacheSocket> setupAction, bool planToConnect)
        {
            IsAlive = false;
            _endPoint = endpoint;
            _authenticator = authenticator;
            _queueTimeout = queueTimeout;
            _setupAction = setupAction;
            _connectTimer = new Timer(TryConnect);
            _initialized = false;
            _pendingLimit = pendingLimit;

            _pendingQueue = new ConcurrentQueue<IMemcacheRequest>();
            _pendingRequests = _pendingLimit > 0 ?
                new BlockingCollection<IMemcacheRequest>(_pendingQueue, _pendingLimit) :
                new BlockingCollection<IMemcacheRequest>(_pendingQueue);

            _sendAsycnhEvtArgs = new SocketAsyncEventArgs();
            _sendAsycnhEvtArgs.Completed += OnSendRequestComplete;

            _receiveHeaderAsycnhEvtArgs = new SocketAsyncEventArgs();
            _receiveHeaderAsycnhEvtArgs.SetBuffer(new byte[MemcacheResponseHeader.SIZE], 0, MemcacheResponseHeader.SIZE);
            _receiveHeaderAsycnhEvtArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnReadResponseComplete);

            _receiveBodyAsycnhEvtArgs = new SocketAsyncEventArgs();
            _receiveBodyAsycnhEvtArgs.Completed += OnReceiveBodyComplete;

            if (planToConnect)
                _connectTimer.Change(0, Timeout.Infinite);
        }

        /// <summary>
        /// Synchronously sends a request
        /// </summary>
        /// <param name="request" />
        public bool TrySend(IMemcacheRequest request)
        {
            if (request == null)
                return false;

            if (!_initialized && !Initialize())
                return false;

            return SendRequest(request);
        }

        /// <summary>
        /// Program a call to the setup action when the transport will be up again
        /// </summary>
        public void PlanSetup()
        {
            if (_initialized && _setupAction != null)
                // the transport is already up => execute now
                _setupAction(this);
        }

        /// <summary>
        /// Dispose the socket
        /// 5 clean tries with 1 second interval, after it dispose it in a more dirty way
        /// </summary>
        public virtual void Dispose()
        {
            // block the start of any resets, then shut down
            _disposed = true;
            int attempt = 0;

            _disposeAttemptTimer = new Timer(socket =>
            {
                try
                {
                    if (_pendingRequests.Count == 0 || ++attempt > 5)
                    {
                        _disposeAttemptTimer.Dispose();
                        ShutDown();
                    }
                }
                catch (Exception e2)
                {
                    if (TransportError != null)
                        TransportError(e2);
                }
            }, null, 1000, 1000);
            _pendingRequests.Dispose();
        }

        private void CreateSocket()
        {
            var socket = new Socket(_endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(_endPoint);

            // Set me in conf
            socket.ReceiveBufferSize = _windowSize;
            socket.SendBufferSize = _windowSize;

            if (_socket != null)
                _socket.Dispose();
            _socket = socket;
        }

        private void FailPending()
        {
            IMemcacheRequest request;
            while (_pendingRequests.TryTake(out request))
                request.Fail();
        }

        private bool Initialize()
        {
            try
            {
                lock (this)
                {
                    if (!_initialized && !_disposed)
                    {
                        CreateSocket();
                        Start();

                        _initialized = true;
                    }
                }
            }
            catch (Exception e2)
            {
                if (TransportError != null)
                    TransportError(e2);

                _connectTimer.Change(1000, Timeout.Infinite);
            }

            return _initialized;
        }

        private IMemcacheRequest UnstackToMatch(MemcacheResponseHeader header)
        {
            IMemcacheRequest result = null;

            if (header.Opcode.IsQuiet())
            {
                throw new MemcacheException("No way we can match a quiet request !");
            }
            else
            {
                // hacky case on partial response for stat command
                if (header.Opcode == Opcode.Stat && header.TotalBodyLength != 0 && header.Status == Status.NoError)
                {
                    if (!_pendingQueue.TryPeek(out result))
                        throw new MemcacheException("Received a response when no request is pending");
                }
                else
                {
                    if (!_pendingRequests.TryTake(out result))
                        throw new MemcacheException("Received a response when no request is pending");
                }

                if (result.RequestId != header.Opaque)
                {
                    result.Fail();
                    throw new MemcacheException("Received a response that doesn't match with the sent request queue : sent " + result.ToString() + " received " + header.ToString());
                }
            }

            return result;
        }

        #region Reads

        private void ReadResponse()
        {
            var socket = _socket;
            try
            {
                _receiveHeaderAsycnhEvtArgs.SetBuffer(0, MemcacheResponseHeader.SIZE);
                if (!socket.ReceiveAsync(_receiveHeaderAsycnhEvtArgs))
                    OnReadResponseComplete(socket, _receiveHeaderAsycnhEvtArgs);
            }
            catch (Exception e)
            {
                if (!_disposed)
                {
                    if (TransportError != null)
                        TransportError(e);
                    socket.Disconnect(false);
                    // don't wait for the error raised by new send to fail pending requests
                    FailPending();
                }
            }
        }

        private void OnReadResponseComplete(object sender, SocketAsyncEventArgs args)
        {
            var socket = (Socket)sender;
            try
            {
                // if the socket has been disposed, don't raise an error
                if (args.SocketError == SocketError.OperationAborted || args.SocketError == SocketError.ConnectionAborted)
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

                ReceiveBody(socket, args.Buffer);
            }
            catch (Exception e)
            {
                if (!_disposed)
                {
                    if (TransportError != null)
                        TransportError(e);
                    socket.Disconnect(false);
                    // don't wait for the error raised by new send to fail pending requests
                    FailPending();
                }
            }
        }
        #endregion Async Reads

        private static ArraySegment<byte> EmptySegment = new ArraySegment<byte>(new byte[0]);
        private void ReceiveBody(Socket socket, byte[] headerBytes)
        {
            var header = new MemcacheResponseHeader(headerBytes);

            var key = header.KeyLength == 0 ? EmptySegment : new ArraySegment<byte>(new byte[header.KeyLength]);
            var extra = header.ExtraLength == 0 ? EmptySegment : new ArraySegment<byte>(new byte[header.ExtraLength]);
            var payloadLength = header.TotalBodyLength - header.KeyLength - header.ExtraLength;
            var payload = payloadLength == 0 ? EmptySegment : new ArraySegment<byte>(new byte[payloadLength]);

            _receiveBodyAsycnhEvtArgs.UserToken = header;
            _receiveBodyAsycnhEvtArgs.BufferList = new ArraySegment<byte>[]
            {
                key,
                extra,
                payload,
            };

            if (header.TotalBodyLength == 0 || !_socket.ReceiveAsync(_receiveBodyAsycnhEvtArgs))
                OnReceiveBodyComplete(_socket, _receiveBodyAsycnhEvtArgs);
        }

        private void OnReceiveBodyComplete(object sender, SocketAsyncEventArgs args)
        {
            var socket = (Socket)sender;
            try
            {
                var header = (MemcacheResponseHeader)args.UserToken;

                // if the socket has been disposed, don't raise an error
                if (args.SocketError == SocketError.OperationAborted || args.SocketError == SocketError.ConnectionAborted)
                    return;
                if (args.SocketError != SocketError.Success)
                    throw new SocketException((int)args.SocketError);

                // check if we read a full header, else continue
                if (args.BytesTransferred + args.Offset < header.TotalBodyLength)
                {
                    int offset = args.BytesTransferred + args.Offset;
                    args.SetBuffer(offset, (int)header.TotalBodyLength - offset);
                    if (!socket.ReceiveAsync(args))
                        OnReadResponseComplete(socket, args);
                    return;
                }
                // should assert we have the good request
                var request = UnstackToMatch(header);

                if (MemcacheResponse != null)
                    MemcacheResponse(header, request);
                if (header.Status != Status.NoError && MemcacheError != null)
                    MemcacheError(header, request);

                var keyBytes = args.BufferList[0].Array;
                var extra = args.BufferList[1].Array;
                var payload = args.BufferList[2].Array;

                if (request != null)
                    request.HandleResponse(header, keyBytes.Length == 0 ? null : UTF8Encoding.Default.GetString(keyBytes), extra, payload);

                // loop the read on the socket
                ReadResponse();
            }
            catch (Exception e)
            {
                if (!_disposed)
                {
                    if (TransportError != null)
                        TransportError(e);
                    socket.Disconnect(false);
                    // don't wait for the error raised by new send to fail pending requests
                    FailPending();
                }
            }
        }

        private void Start()
        {
            _token = new CancellationTokenSource();

            ReadResponse();
            Authenticate();

            IsAlive = true;
        }

        private void ShutDown()
        {
            IsAlive = false;
            try
            {
                if (_token != null)
                    _token.Cancel();
            }
            catch (AggregateException e)
            {
                if (TransportError != null)
                    TransportError(e);
            }

            var socket = _socket;
            if (socket != null)
                socket.Dispose();

            if (_receiveHeaderAsycnhEvtArgs != null)
                _receiveHeaderAsycnhEvtArgs.Dispose();

            if (TransportDead != null)
                TransportDead(this);
            FailPending();
        }

        private bool Authenticate()
        {
            bool authDone = false;
            IMemcacheRequest request = null;
            Status authStatus = Status.NoError;

            if (_authenticator != null)
            {
                var authenticationToken = _authenticator.CreateToken();
                while (authenticationToken != null && !authDone)
                {
                    authStatus = authenticationToken.StepAuthenticate(out request);

                    switch (authStatus)
                    {
                        // auth OK, clear the token
                        case Status.NoError:
                            authenticationToken = null;
                            authDone = true;
                            break;
                        case Status.StepRequired:
                            if (request == null)
                                throw new AuthenticationException("Unable to authenticate : step required but no request from token");
                            if (!SendRequest(request))
                                throw new AuthenticationException("Unable to authenticate : unable to send authentication request");
                            break;
                        default:
                            throw new AuthenticationException("Unable to authenticate : status " + authStatus.ToString());
                    }
                }
            }

            return true;
        }

        private void TryConnect(object dummy)
        {
            if (Initialize() && _setupAction != null)
                _setupAction(this);
        }


        private bool SendAsynch(byte[] buffer, int offset, int count)
        {
            try
            {
                _sendAsycnhEvtArgs.SetBuffer(buffer, offset, count);
                if (!_socket.SendAsync(_sendAsycnhEvtArgs))
                {
                    OnSendRequestComplete(_socket, _sendAsycnhEvtArgs);
                    return false;
                }
            }
            catch (Exception e)
            {
                if (!_disposed)
                {
                    if (TransportError != null)
                        TransportError(e);
                    ShutDown();

                    new MemcacheSocket(_endPoint, _authenticator, _queueTimeout, _pendingLimit, _setupAction, true);
                }
            }

            return true;
        }

        private void OnSendRequestComplete(object sender, SocketAsyncEventArgs args)
        {
            try
            {
                var socket = (Socket)sender;

                // if the socket has been disposed, don't raise an error
                if (args.SocketError == SocketError.OperationAborted || args.SocketError == SocketError.ConnectionAborted)
                    return;
                if (args.SocketError != SocketError.Success)
                    throw new SocketException((int)args.SocketError);

                // check if we read a full header, else continue
                if (args.BytesTransferred + args.Offset < args.Buffer.Length)
                {
                    int offset = args.BytesTransferred + args.Offset;
                    args.SetBuffer(offset, args.Buffer.Length - offset);
                    if (!socket.SendAsync(args))
                        OnSendRequestComplete(socket, args);
                    return;
                }

                _setupAction(this);
            }
            catch (Exception e)
            {
                if (!_disposed)
                {
                    if (TransportError != null)
                        TransportError(e);
                    ShutDown();

                    new MemcacheSocket(_endPoint, _authenticator, _queueTimeout, _pendingLimit, _setupAction, true);
                }
            }
        }

        private bool SendRequest(IMemcacheRequest request)
        {
            try
            {
                var buffer = request.GetQueryBuffer();

                try
                {
                    if (!_pendingRequests.TryAdd(request, _queueTimeout, _token.Token))
                    {
                        if (TransportError != null)
                            TransportError(new MemcacheException("Send request queue full to " + _endPoint));

                        _setupAction(this);
                        return false;
                    }
                }
                catch (OperationCanceledException e)
                {
                    if (TransportError != null)
                        TransportError(e);
                    return false;
                }

                return SendAsynch(buffer, 0, buffer.Length);
            }
            catch (Exception e)
            {
                if (TransportError != null)
                    TransportError(e);

                ShutDown();

                return false;
            }
        }

        public bool Registered { get { return TransportDead != null; } }
    }
}
