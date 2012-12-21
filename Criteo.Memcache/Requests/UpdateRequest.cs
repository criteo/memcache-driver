using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    class UpdateRequest : SetRequest
    {
        public UpdateRequest()
            : base()
        {
        }

        protected override Opcode Code
        {
            get
            {
                return Opcode.Replace;
            }
        }
    }
}
