using System.Text;

namespace Criteo.Memcache.KeySerializer
{
    public class UTF8KeySerializer : KeySerializerWithChecks<string>
    {
        protected override byte[] DoSerializeToBytes(string value)
        {
            if (value == null)
                return null;

            return UTF8Encoding.Default.GetBytes(value);
        }
    }
}
