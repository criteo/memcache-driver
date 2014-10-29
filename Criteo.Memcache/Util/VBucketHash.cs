namespace Criteo.Memcache.Util
{
    /// <summary>
    /// This is a shamelesss rewrite from Couchbase's .Net client.
    /// While they call it CRC32 it returns the value crc32[30..16],
    /// and thus should be named appropriately (see CCBC-208 on Couchbase's JIRA).
    /// </summary>
    internal sealed class VBucketHash
    {
        private const uint Polynomial = 0xedb88320u;
        private const uint Seed = 0xffffffffu;
        private static readonly uint[] Table = new uint[256];

        static VBucketHash()
        {
            for (var i = 0u; i < Table.Length; ++i)
            {
                var temp = i;
                for (var j = 8u; j > 0; --j)
                    if ((temp & 1) == 1)
                        temp = ((temp >> 1) ^ Polynomial);
                    else
                        temp >>= 1;

                Table[i] = temp;
            }
        }

        public uint Compute(byte[] array)
        {
            var hash = Seed;
            foreach (var octet in array)
                hash = (hash >> 8) ^ Table[octet ^ hash & 0xff];

            return ((~hash) >> 16) & 0x7fff;
        }
    }
}

#region License information
/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2014 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/
#endregion