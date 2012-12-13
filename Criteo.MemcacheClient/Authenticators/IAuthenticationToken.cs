using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.MemcacheClient.Headers;
using Criteo.MemcacheClient.Requests;

namespace Criteo.MemcacheClient.Authenticators
{
    public interface IAuthenticationToken
    {
        /// <summary>
        /// This method returns the authentication status and the next request for authentication
        /// </summary>
        /// <param name="stepRequest">The next request to send for the authentication process</param>
        /// <returns>
        /// NoError when authentication is done
        /// StepRequired when a next step must be sent
        /// AuthRequired when the authentication has failed
        /// </returns>
        Status StepAuthenticate(out IMemcacheRequest stepRequest);
    }
}
