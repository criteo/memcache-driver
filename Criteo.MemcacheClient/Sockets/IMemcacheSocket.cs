using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.Sockets
{
    public interface IMemcacheSocket
    {
        /// <summary>
        /// This event is triggered when an exception occures
        /// </summary>
        event Action<Exception> TransportError;

        /// <summary>
        /// This event is triggered every time a respond from the server is not ok
        /// </summary>
        event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheError;

        /// <summary>
        /// This event is triggered at every incoming response from the server
        /// </summary>
        event Action<MemacheResponseHeader, IMemcacheRequest> MemcacheResponse;
    }
}
