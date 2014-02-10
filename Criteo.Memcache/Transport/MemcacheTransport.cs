/* Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Criteo.Memcache.Requests;
using Criteo.Memcache.Configuration;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Exceptions;

namespace Criteo.Memcache.Transport
{
    internal class MemcacheTransport : IMemcacheTransport
    {
        #region Events

        public event Action<Exception> TransportError;

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheError;

        public event Action<MemcacheResponseHeader, IMemcacheRequest> MemcacheResponse;

        public event Action<IMemcacheTransport> TransportDead;

        #endregion Events

        // TODO : add me in the Conf
        private readonly int _windowSize = 2 << 15;

        private readonly EndPoint _endPoint;
        private readonly MemcacheClientConfiguration _clientConfig;

        private readonly Action<IMemcacheTransport> _registerEvents;
        private readonly Action<IMemcacheTransport> _transportAvailable;
        private readonly IOngoingDispose _nodeDispose;

        private readonly Timer _connectTimer;

        private volatile bool _disposed = false;
        private volatile bool _initialized = false;
        private int _transportAvailableInReceive = 0;
        private ConcurrentQueue<IMemcacheRequest> _pendingRequests;
        private Socket _socket;
        private CancellationTokenSource _token;

        private SocketAsyncEventArgs _sendAsynchEvtArgs;
        private SocketAsyncEventArgs _receiveHeaderAsynchEvtArgs;
        private SocketAsyncEventArgs _receiveBodyAsynchEvtArgs;

        private MemcacheResponseHeader _currentResponse;

        public bool IsAlive { get; private set; }

        // The Registered property is set to true by the memcache node
        // when it acknowledges the transport is a working transport.
        public bool Registered { get; set; }

        // Default transport allocator
        public static TransportAllocator DefaultAllocator =
            (endPoint, config, register, available, autoConnect,dispose)
                => new MemcacheTransport(endPoint, config, register, available, autoConnect, dispose);

        /// <summary>
        /// Ctor, intialize things ...
        /// </summary>
        /// <param name="endpoint" />
        /// <param name="clientConfig">The client configuration</param>
        /// <param name="registerEvents">Delegate to call to register the transport</param>
        /// <param name="tranportAvailable">Delegate to call when the transport is alive</param>
        /// <param name="planToConnect">If true, connect in a timer handler started immediately and call transportAvailable.
        ///                             Otherwise, the transport will connect synchronously at the first request</param>
        /// <param name="nodeDispose">Interface to check if the node is being disposed"</param>
        public MemcacheTransport(EndPoint endpoint, MemcacheClientConfiguration clientConfig, Action<IMemcacheTransport> registerEvents, Action<IMemcacheTransport> transportAvailable, bool planToConnect, IOngoingDispose nodeDispose)
        {
            if (clientConfig == null)
                throw new ArgumentException("Client config should not be null");

            IsAlive = false;
            _endPoint = endpoint;
            _clientConfig = clientConfig;
            _registerEvents = registerEvents;
            _transportAvailable = transportAvailable;
            _nodeDispose = nodeDispose;

            _connectTimer = new Timer(TryConnect);
            _initialized = false;

            _registerEvents(this);

            _pendingRequests = new ConcurrentQueue<IMemcacheRequest>();

            _sendAsynchEvtArgs = new SocketAsyncEventArgs();
            _sendAsynchEvtArgs.Completed += OnSendRequestComplete;

            _receiveHeaderAsynchEvtArgs = new SocketAsyncEventArgs();
            _receiveHeaderAsynchEvtArgs.SetBuffer(new byte[MemcacheResponseHeader.SIZE], 0, MemcacheResponseHeader.SIZE);
            _receiveHeaderAsynchEvtArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnReceiveHeaderComplete);

            _receiveBodyAsynchEvtArgs = new SocketAsyncEventArgs();
            _receiveBodyAsynchEvtArgs.Completed += OnReceiveBodyComplete;

            if (planToConnect)
                _connectTimer.Change(0, Timeout.Infinite);
        }

        /// <summary>
        /// Synchronously sends a request
        /// </summary>
        /// <param name="request" />
        public bool TrySend(IMemcacheRequest request)
        {
            if (request == null || _disposed)
                return false;

            if (!_initialized && !Initialize())
                return false;

            return SendRequest(request);
        }

        /// <summary>
        /// Dispose the socket
        /// 5 clean tries with 1 second interval, after it disposes it in a more dirty way
        /// </summary>
        public virtual void Dispose()
        {
            // block the start of any resets, then shut down
            if (!_disposed)
                lock (this)
                    if (!_disposed)
                    {
                        if (TransportDead != null)
                            TransportDead(this);

                        int attempt = 0;

                        while (true)
                        {
                            try
                            {
                                if (_pendingRequests.Count == 0 || ++attempt > 5)
                                {
                                    ShutDown();
                                    _disposed = true;
                                    break;
                                }
                            }
                            catch (Exception e2)
                            {
                                if (TransportError != null)
                                    TransportError(e2);
                                _disposed = true;
                                break;
                            }

                            Thread.Sleep(1000);
                        }
                    }
        }

        private void CreateSocket()
        {
            var socket = new Socket(_endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(_endPoint);

            socket.ReceiveBufferSize = _windowSize;
            socket.SendBufferSize = _windowSize;

            if (_socket != null)
                _socket.Dispose();
            _socket = socket;
        }

        private void FailPending()
        {
            IMemcacheRequest request;
            while (_pendingRequests.TryDequeue(out request))
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

                _connectTimer.Change(Convert.ToInt32(_clientConfig.TransportConnectTimerPeriod.TotalMilliseconds), Timeout.Infinite);

                return false;
            }

            return true;
        }

        private IMemcacheRequest DequeueToMatch(MemcacheResponseHeader header)
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
                    if (!_pendingRequests.TryPeek(out result))
                        throw new MemcacheException("Received a response when no request is pending");
                }
                else
                {
                    if (!_pendingRequests.TryDequeue(out result))
                        throw new MemcacheException("Received a response when no request is pending");
                }

                if (result.RequestId != header.Opaque)
                {
                    result.Fail();
                    throw new MemcacheException("Received a response that doesn't match with the sent request queue : sent " + result.ToString() + " received " + header.ToString());
                }
            }

            AvailableInReceive();
            return result;
        }

        #region Reads

        private void ReceiveResponse()
        {
            try
            {
                _receiveHeaderAsynchEvtArgs.SetBuffer(0, MemcacheResponseHeader.SIZE);
                if (!_socket.ReceiveAsync(_receiveHeaderAsynchEvtArgs))
                    OnReceiveHeaderComplete(_socket, _receiveHeaderAsynchEvtArgs);
            }
            catch (Exception e)
            {
                TransportFailureOnReceive(e);
            }
        }

        private void OnReceiveHeaderComplete(object sender, SocketAsyncEventArgs args)
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
                        OnReceiveHeaderComplete(socket, args);
                    return;
                }

                ReceiveBody(socket, args.Buffer);
            }
            catch (Exception e)
            {
                TransportFailureOnReceive(e);
                // if the receive header failed, we must restack the transport cause only sends detects it
            }
        }

        #endregion Reads

        private static ArraySegment<byte> EmptySegment = new ArraySegment<byte>(new byte[0]);

        private void ReceiveBody(Socket socket, byte[] headerBytes)
        {
            _currentResponse = new MemcacheResponseHeader(headerBytes);

            var body = _currentResponse.TotalBodyLength == 0 ? null : new byte[_currentResponse.TotalBodyLength];

            _receiveBodyAsynchEvtArgs.SetBuffer(body, 0, (int)_currentResponse.TotalBodyLength);

            try
            {
                if (_currentResponse.TotalBodyLength == 0 || !_socket.ReceiveAsync(_receiveBodyAsynchEvtArgs))
                    OnReceiveBodyComplete(_socket, _receiveBodyAsynchEvtArgs);
            }
            catch (Exception e)
            {
                TransportFailureOnReceive(e);
            }
        }

        private void OnReceiveBodyComplete(object sender, SocketAsyncEventArgs args)
        {
            var socket = (Socket)sender;
            try
            {
                // if the socket has been disposed, don't raise an error
                if (args.SocketError == SocketError.OperationAborted || args.SocketError == SocketError.ConnectionAborted)
                    return;
                if (args.SocketError != SocketError.Success)
                    throw new SocketException((int)args.SocketError);

                // check if we read a full body, else continue
                if (args.BytesTransferred + args.Offset < _currentResponse.TotalBodyLength)
                {
                    int offset = args.BytesTransferred + args.Offset;
                    args.SetBuffer(offset, (int)_currentResponse.TotalBodyLength - offset);
                    if (!socket.ReceiveAsync(args))
                        OnReceiveHeaderComplete(socket, args);
                    return;
                }
                // should assert we have the good request
                var request = DequeueToMatch(_currentResponse);

                if (MemcacheResponse != null)
                    MemcacheResponse(_currentResponse, request);
                if (_currentResponse.Status != Status.NoError && MemcacheError != null)
                    MemcacheError(_currentResponse, request);

                byte[] extra = null;
                if (_currentResponse.ExtraLength == _currentResponse.TotalBodyLength)
                    extra = args.Buffer;
                else if (_currentResponse.ExtraLength > 0)
                {
                    extra = new byte[_currentResponse.ExtraLength];
                    Array.Copy(args.Buffer, 0, extra, 0, _currentResponse.ExtraLength);
                }

                string key = null;
                if (_currentResponse.KeyLength > 0)
                    key = UTF8Encoding.Default.GetString(args.Buffer, _currentResponse.ExtraLength, _currentResponse.KeyLength);

                var payloadLength = _currentResponse.TotalBodyLength - _currentResponse.KeyLength - _currentResponse.ExtraLength;
                byte[] payload = null;
                if (payloadLength == _currentResponse.TotalBodyLength)
                    payload = args.Buffer;
                else if (payloadLength > 0)
                {
                    payload = new byte[payloadLength];
                    Array.Copy(args.Buffer, _currentResponse.KeyLength + _currentResponse.ExtraLength, payload, 0, payloadLength);
                }

                if (request != null)
                    request.HandleResponse(_currentResponse, key, extra, payload);

                // loop the read on the socket
                ReceiveResponse();
            }
            catch (Exception e)
            {
                TransportFailureOnReceive(e);
            }
        }

        private void AvailableInReceive()
        {
            if (1 == Interlocked.CompareExchange(ref _transportAvailableInReceive, 0, 1))
            {
                // the flag has been successfully reset
                TransportAvailable();
            }
        }

        private void TransportFailureOnReceive(Exception e)
        {
            if (!_disposed)
                lock (this)
                    if (!_disposed)
                    {
                        _socket.Disconnect(false);
                        if (TransportError != null)
                        {
                            TransportError(e);
                        }

                        FailPending();
                    }
            AvailableInReceive();
        }

        private void Start()
        {
            _token = new CancellationTokenSource();

            ReceiveResponse();
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

            if (_receiveHeaderAsynchEvtArgs != null)
                _receiveHeaderAsynchEvtArgs.Dispose();

            FailPending();
        }

        private bool Authenticate()
        {
            bool authDone = false;
            IMemcacheRequest request = null;
            Status authStatus = Status.NoError;

            if (_clientConfig.Authenticator != null)
            {
                var mre = new ManualResetEventSlim();
                var authenticationToken = _clientConfig.Authenticator.CreateToken();
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
                            if (!SendRequest(request, mre))
                                throw new AuthenticationException("Unable to authenticate : unable to send authentication request");
                            break;

                        default:
                            throw new AuthenticationException("Unable to authenticate : status " + authStatus.ToString());
                    }
                }
                mre.Wait();
            }

            return true;
        }

        private void TryConnect(object dummy)
        {
            try
            {
                // If the node has been disposed, dispose this transport, which will terminate the reconnect timer.
                if (_nodeDispose != null &&
                    _nodeDispose.OngoingDispose)
                {
                    Dispose();
                }
                // Else, try to connect and register the transport on the node in case of success
                else if (Initialize())
                {
                    TransportAvailable();
                }
            }
            catch (Exception e)
            {
                if (TransportError != null)
                    TransportError(e);

                _connectTimer.Change(Convert.ToInt32(_clientConfig.TransportConnectTimerPeriod.TotalMilliseconds), Timeout.Infinite);
            }
        }

        private bool SendAsynch(byte[] buffer, int offset, int count, ManualResetEventSlim callAvailable)
        {
            _sendAsynchEvtArgs.UserToken = callAvailable;

            _sendAsynchEvtArgs.SetBuffer(buffer, offset, count);
            if (!_socket.SendAsync(_sendAsynchEvtArgs))
            {
                OnSendRequestComplete(_socket, _sendAsynchEvtArgs);
                return false;
            }

            return true;
        }

        private void OnSendRequestComplete(object sender, SocketAsyncEventArgs args)
        {
            try
            {
                var socket = (Socket)sender;

                if (args.SocketError != SocketError.Success)
                    throw new SocketException((int)args.SocketError);

                // check if we sent a full request, else continue
                if (args.BytesTransferred + args.Offset < args.Buffer.Length)
                {
                    int offset = args.BytesTransferred + args.Offset;
                    args.SetBuffer(offset, args.Buffer.Length - offset);
                    if (!socket.SendAsync(args))
                        OnSendRequestComplete(socket, args);
                    return;
                }

                if (args.UserToken == null)
                    TransportAvailable();
                else
                    (args.UserToken as ManualResetEventSlim).Set();
            }
            catch (Exception e)
            {
                TransportFailureOnSend(e);
            }
        }

        private bool SendRequest(IMemcacheRequest request, ManualResetEventSlim callAvailable = null)
        {
            byte[] buffer;
            try
            {
                buffer = request.GetQueryBuffer();

                if(_clientConfig.QueueLength > 0 &&
                    _pendingRequests.Count >= _clientConfig.QueueLength)
                {
                    // The request queue is full, the transport will be put back in the pool after the queue is not full anymore
                    Interlocked.Exchange(ref _transportAvailableInReceive, 1);
                    if (!_pendingRequests.IsEmpty)
                        // the receive will reset the flag after the next dequeue
                        return false;

                    if (0 == Interlocked.CompareExchange(ref _transportAvailableInReceive, 0, 1))
                        // the flag has already been reset (by the receive)
                        return false;
                }

                _pendingRequests.Enqueue(request);
                SendAsynch(buffer, 0, buffer.Length, callAvailable);
            }
            catch (Exception e)
            {
                TransportFailureOnSend(e);
                return false;
            }

            return true;
        }

        // Register the transport on the node.
        // The caller must catch exceptions and possibly, check the node's IOngoingDispose interface beforehand
        private void TransportAvailable()
        {
            if (_transportAvailable != null)
                _transportAvailable(this);
        }

        private void TransportFailureOnSend(Exception e)
        {
            if (!_disposed)
                lock (this)
                    if (!_disposed)
                    {
                        if (TransportError != null)
                            TransportError(e);

                        // If the node hasn't been disposed, allocate a new transport that will attempt to reconnect
                        if (!(_nodeDispose != null &&
                              _nodeDispose.OngoingDispose == true))
                        {
                            var newTransport =  (_clientConfig.TransportFactory ?? DefaultAllocator)(_endPoint, _clientConfig, _registerEvents, _transportAvailable, true, _nodeDispose);
                        }

                        // Dispose this transport
                        FailPending();
                        Dispose();
                    }
        }

        public override string ToString()
        {
            return "MemcacheTransport " + _endPoint + " " + GetHashCode();
        }
    }
}
