using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Criteo.Memcache.Exceptions
{
    class ConfigurationException : MemcacheException
    {
        public ConfigurationException(string message)
            : base(message)
        {
        }
    }
}
