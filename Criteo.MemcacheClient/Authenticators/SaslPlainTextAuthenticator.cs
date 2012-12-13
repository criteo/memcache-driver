using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Criteo.MemcacheClient.Sockets;
using Criteo.MemcacheClient.Node;
using Criteo.MemcacheClient.Requests;
using Criteo.MemcacheClient.Headers;

namespace Criteo.MemcacheClient.Authenticators
{
    internal class SaslPlainTextAuthenticator : IMemcacheAuthenticator
    {
        public string Zone { get; set; }
        public string User { get; set; }
        public string Password { get; set; }

        private class SaslPlainTextToken : IAuthenticationToken
        {
            private TaskCompletionSource<Status> _authenticationStatus;
            private IMemcacheRequest _request;
            private bool _started = false;

            public SaslPlainTextToken(string zone, string user, string password)
            {
                _authenticationStatus = new TaskCompletionSource<Status>();
                _request = new SaslPlainRequest
                {
                    Zone = zone,
                    User = user,
                    Password = password,
                    Callback = _authenticationStatus.SetResult,
                };
            }

            public Status StepAuthenticate(out IMemcacheRequest stepRequest)
            {
                if (_started)
                {
                    stepRequest = null;
                    var status = _authenticationStatus.Task.Result;
                    // unexpected, let's returns again the authentication request ...
                    if(status == Status.StepRequired)
                        stepRequest = _request;
                    return status;
                }
                else
                {
                    _started = true;
                    stepRequest = _request;
                    return Status.StepRequired;
                }
            }
        }

        public IAuthenticationToken CreateToken()
        {
            return new SaslPlainTextToken(Zone, User, Password);
        }
    }
}
