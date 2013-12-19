using System;

namespace Criteo.Memcache.Exceptions
{
    internal class MemcacheException : Exception
    {
        public MemcacheException(string message)
            : base(message)
        {
        }
    }
}
