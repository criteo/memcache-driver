using System;

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
