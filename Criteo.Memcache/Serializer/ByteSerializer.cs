using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.Memcache.Serializer
{
    class ByteSerializer : ISerializer<byte[]>
    {
        private const uint RawDataFlag = 0xfa52;

        public byte[] ToBytes(byte[] value)
        {
            return value;
        }

        public byte[] FromBytes(byte[] value)
        {
            return value;
        }

        public uint TypeFlag { get { return RawDataFlag; } }
    }
}
