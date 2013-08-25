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
        protected Action<Exception> _transportError;
        public event Action<Exception> TransportError
        {
            add { _transportError += value; }
            remove { _transportError -= value; }
        }

        protected Action<MemcacheResponseHeader, IMemcacheRequest> _memcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError
        {
            add { _memcacheError += value; }
            remove { _memcacheError -= value; }
        }

        protected Action<MemcacheResponseHeader, IMemcacheRequest> _memcacheResponse;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse
        {
            add { _memcacheResponse += value; }
            remove { _memcacheResponse -= value; }
        }
        #endregion Event

        private readonly bool _threadedReceived;
        private readonly int _queueTimeout;
        // TODO : add me in the Conf
        private readonly int _requestLimit = 1000;
        private readonly int _windowSize = 2 << 15;
        // END TODO
        private readonly EndPoint _endPoint;
        private readonly IMemcacheAuthenticator _authenticator;
        private readonly Action<MemcacheSocket> _setupAction;
        private readonly Timer _reconnectTimer;

        private volatile bool _disposed = false;
        private volatile bool _initialized = false;
        private BlockingCollection<IMemcacheRequest> _pendingRequests;
        private ConcurrentQueue<IMemcacheRequest> _pendingQueue;
        private Socket _socket;
        private CancellationTokenSource _token;
        private Timer _disposeAttemptTimer;

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
        public MemcacheSocket(EndPoint endpoint, IMemcacheAuthenticator authenticator, int queueTimeout, int pendingLimit, Action<MemcacheSocket> setupAction, bool threaded, bool planToConnect)
        {
            IsAlive = false;
            _endPoint = endpoint;
            _authenticator = authenticator;
            _queueTimeout = queueTimeout;
            _setupAction = setupAction;
            _threadedReceived = threaded;
            _reconnectTimer = new Timer(TryReconnect);
            _initialized = false;

            _pendingQueue = new ConcurrentQueue<IMemcacheRequest>();
            _pendingRequests = _requestLimit > 0 ?
                new BlockingCollection<IMemcacheRequest>(_pendingQueue, _requestLimit) :
                new BlockingCollection<IMemcacheRequest>(_pendingQueue);


            if (planToConnect)
                _reconnectTimer.Change(0, Timeout.Infinite);
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
                        ShutDown();
                    }
                }
                catch (Exception e2)
                {
                    if (_transportError != null)
                        _transportError(e2);
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
                if (_transportError != null)
                    _transportError(e2);

                _reconnectTimer.Change(1000, Timeout.Infinite);
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

        #region Threaded Reads
        private Thread _receivingThread;

        private void StartReceivingThread()
        {
            _receivingThread = new Thread(ReceiveJob);
            _receivingThread.Start(_token.Token);
        }

        private void ReceiveJob(object t)
        {
            var buffer = new byte[MemcacheResponseHeader.SIZE];
            var token = (CancellationToken)t;
            while (!token.IsCancellationRequested)
            {
                var socket = _socket;
                try
                {
                    int received = 0;
                    do
                    {
                        var localReceived = socket.Receive(buffer, received, MemcacheResponseHeader.SIZE - received, SocketFlags.None);
                        if (localReceived == 0)
                            throw new MemcacheException("The remote closed the connection unexpectedly");
                        received += localReceived;
                    } while (received < MemcacheResponseHeader.SIZE);

                    ReceiveBody(socket, buffer);
                }
                catch (Exception e)
                {
                    if (!token.IsCancellationRequested && !_disposed)
                    {
                        if (_transportError != null)
                            _transportError(e);

                        socket.Disconnect(false);
                        // don't wait for the error raised by new send to fail pending requests
                        FailPending();
                    }
                }
            }
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
            var socket = _socket;
            try
            {
                _receiveArgs.SetBuffer(0, MemcacheResponseHeader.SIZE);
                if (!socket.ReceiveAsync(_receiveArgs))
                    OnReadResponseComplete(socket, _receiveArgs);
            }
            catch (Exception e)
            {
                if (!_disposed)
                {
                    if (_transportError != null)
                        _transportError(e);
                    socket.Disconnect(false);
                    // don't wait for the error raised by new send to fail pending requests
                    FailPending();
                }
            }
        }

        private void OnReadResponseComplete(object sender, SocketAsyncEventArgs args)
        {
            var socket = sender as Socket;
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

                // loop the read on the socket
                ReadResponse();
            }
            catch (Exception e)
            {
                if (!_disposed)
                {
                    if (_transportError != null)
                        _transportError(e);
                    socket.Disconnect(false);
                    // don't wait for the error raised by new send to fail pending requests
                    FailPending();
                }
            }
        }
        #endregion Async Reads

        private void ReceiveBody(Socket socket, byte[] headerBytes)
        {
            var header = new MemcacheResponseHeader(headerBytes);

            var key = Receive(socket, (int)header.KeyLength, 0);
            var extra = Receive(socket, (int)header.ExtraLength, 0);
            var payload = Receive(socket, (int)(header.TotalBodyLength - header.KeyLength - header.ExtraLength), 0);

            // should assert we have the good request
            var request = UnstackToMatch(header);

            if (_memcacheResponse != null)
                _memcacheResponse(header, request);
            if (header.Status != Status.NoError && _memcacheError != null)
                _memcacheError(header, request);

            if (request != null)
                request.HandleResponse(header, key == null ? null : UTF8Encoding.Default.GetString(key), extra, payload);
        }

        private byte[] Receive(Socket socket, int size, int offset = 0)
        {
            byte[] target = null;

            if (size > 0)
            {
                target = new byte[size];
                int received = 0;
                do
                {
                    var localReceived = socket.Receive(target, offset + received, size - received, SocketFlags.None);
                    if (localReceived == 0)
                        throw new MemcacheException("The remote closed the connection unexpectedly");
                    received += localReceived;
                } while (received < size);
            }
            
            return target;
        }

        private void Start()
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
                if (_transportError != null)
                    _transportError(e);
            }

            var socket = _socket;
            if (socket != null)
                socket.Dispose();

            if (_receiveArgs != null)
                _receiveArgs.Dispose();

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

        private void TryReconnect(object dummy)
        {
            if (Initialize() && _setupAction != null)
                _setupAction(this);
        }

        private bool SendRequest(IMemcacheRequest request)
        {
            var socket = _socket;
            try
            {
                var buffer = request.GetQueryBuffer();

                try
                {
                    if (!_pendingRequests.TryAdd(request, _queueTimeout, _token.Token))
                    {
                        if (_transportError != null)
                            _transportError(new MemcacheException("Send request queue full to " + _endPoint));
                        return false;
                    }
                }
                catch (OperationCanceledException e)
                {
                    if (_transportError != null)
                        _transportError(e);
                    return false;
                }

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

                ShutDown();

                return false;
            }
        }
    }
}
