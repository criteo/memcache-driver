using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Sockets;
using Criteo.MemcacheClient.Headers;
using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.Authenticators
{
    public interface IMemcacheAuthenticator
    {
        /// <summary>
        /// Create an authentication token for a single connection
        /// </summary>
        /// <returns>A new token for authenticating the connection</returns>
        IAuthenticationToken CreateToken();
    }
}
