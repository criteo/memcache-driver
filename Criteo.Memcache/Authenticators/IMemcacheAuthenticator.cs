using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Transport;
using Criteo.Memcache.Headers;
using Criteo.Memcache.Requests;

namespace Criteo.Memcache.Authenticators
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
