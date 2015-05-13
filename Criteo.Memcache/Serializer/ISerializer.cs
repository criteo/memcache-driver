using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.Memcache.Serializer
{
    public interface ISerializer
    {
        uint TypeFlag { get; }
    }

    public interface ISerializer<T> : ISerializer
    {
        byte[] ToBytes(T value);
        T FromBytes(byte[] value);
    }
}
