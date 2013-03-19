using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.Memcache.Exceptions
{
    internal class AuthenticationException : Exception
    {
        public AuthenticationException(string message)
            : base(message)
        {
        }
    }
}
