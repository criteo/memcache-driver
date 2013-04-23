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
            add
            {
                _transportError += value;
            }
            remove
            {
                _transportError -= value;
            }
        }

        protected Action<MemcacheResponseHeader, IMemcacheRequest> _memcacheError;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError
        {
            add
            {
                _memcacheError += value;
            }
            remove
            {
                _memcacheError -= value;
            }
        }

        protected Action<MemcacheResponseHeader, IMemcacheRequest> _memcacheResponse;
        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse
        {
            add
            {
                _memcacheResponse += value;
            }
            remove
            {
                _memcacheResponse -= value;
            }
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

        private bool _disposed = false;
        private bool _initialized = false;
        private BlockingCollection<IMemcacheRequest> _pendingRequests;
        private ConcurrentQueue<IMemcacheRequest> _pendingQueue;
        private Socket _socket;
        private CancellationTokenSource _token;
        private Timer _disposeAttemptTimer;

        /// <summary>
        /// Ctor, intialize things ...
        /// </summary>
        /// <param name="endpoint" />
        /// <param name="authenticator">Object that ables to Sasl authenticate the socket</param>
        /// <param name="queueTimeout" />
        /// <param name="pendingLimit" />
        /// <param name="setupAction">Delegate to call when the transport is alive</param>
        /// <param name="threaded">If true use a thread to synchronously receive on the socket else use the asynchronous API</param>
        public MemcacheSocket(EndPoint endpoint, IMemcacheAuthenticator authenticator, int queueTimeout, int pendingLimit, Action<MemcacheSocket> setupAction, bool threaded)
        {
            _endPoint = endpoint;
            _authenticator = authenticator;
            _queueTimeout = queueTimeout;
            _setupAction = setupAction;
            _threadedReceived = threaded;
            _reconnectTimer = new Timer(TryReconnect);

            Reset(null);
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
            lock (this)
            {
                _disposed = true;
                int attempt = 0;


                _disposeAttemptTimer = new Timer(socket =>
                {
                    try
                    {
                        lock (this)
                        {
                            if (_pendingRequests.Count == 0 || ++attempt > 5)
                            {
                                try
                                {
                                    ShutDown();
                                }
                                finally
                                {
                                    // fail of pending requests
                                    if (_pendingRequests != null)
                                    {
                                        DisposePending(_pendingRequests);
                                        _pendingRequests = null;
                                    }
                                    _disposeAttemptTimer.Dispose();
                                    _disposeAttemptTimer = null;
                                }
                            }
                        }
                    }
                    catch (Exception e2)
                    {
                        if (_transportError != null)
                            _transportError(e2);
                    }
                }, null, 1000, 1000);

            }
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

        private void Reset(Socket socketFailing)
        {
            BlockingCollection<IMemcacheRequest> oldPending = null;

            lock (this)
            {
                // the transport has been disposed or the socket has already changed
                if (_disposed ||
                    (_socket != null && !object.ReferenceEquals(socketFailing, _socket)))
                    return;

                // somthing goes wrong, stop to send
                ShutDown();
                _initialized = false;

                // keep the pending request somewhere
                Interlocked.Exchange(ref _pendingQueue, new ConcurrentQueue<IMemcacheRequest>());
                var newPending = _requestLimit > 0 ?
                    new BlockingCollection<IMemcacheRequest>(_pendingQueue, _requestLimit) :
                    new BlockingCollection<IMemcacheRequest>(_pendingQueue);
                oldPending = Interlocked.Exchange(ref _pendingRequests, newPending);

                // we are in a case when the previous socket failed, don't synchronously reconnect
                if (_socket != null)
                    _reconnectTimer.Change(0, Timeout.Infinite);
            }

            if (oldPending != null)
                DisposePending(oldPending);
        }

        private void DisposePending(BlockingCollection<IMemcacheRequest> pending)
        {
            IMemcacheRequest request;
            while (pending.TryTake(out request))
                request.Fail();
            pending.Dispose();
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
                    throw new MemcacheException("Received a response that doesn't match with the sent request queue");
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

                        Reset(socket);
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
                    Reset(socket);
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
        }

        private void ShutDown()
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

            var socket = _socket;
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
                            {
                                if (_transportError != null)
                                    _transportError(new AuthenticationException("Unable to authenticate : step required but no request from token"));
                                Reset(_socket);
                                return false;
                            }
                            return SendRequest(request);
                        default:
                            if (_transportError != null)
                                _transportError(new AuthenticationException("Unable to authenticate : status " + authStatus.ToString()));
                            Reset(_socket);
                            return false;
                    }
                }
            }

            return true;
        }

        private void TryReconnect(object dummy)
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

            if (reconnected && _setupAction != null)
                _setupAction(this);
        }

        private bool SendRequest(IMemcacheRequest request)
        {
            var socket = _socket;
            try
            {
                var buffer = request.GetQueryBuffer();

                if (!_pendingRequests.TryAdd(request, _queueTimeout, _token.Token))
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
    }
}
