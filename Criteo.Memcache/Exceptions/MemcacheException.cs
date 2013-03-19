using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
