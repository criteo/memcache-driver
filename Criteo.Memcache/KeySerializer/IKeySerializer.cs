using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.Memcache.KeySerializer
{
    public interface IKeySerializer<KeyType>
    {
        byte[] SerializeToBytes(KeyType value);
    }
}
