using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Criteo.Memcache.Headers;

namespace Criteo.Memcache.Requests
{
    class AddRequest : SetRequest
    {
        public AddRequest()
            : base()
        {
        }

        protected override Opcode Code
        {
            get
            {
                return Opcode.Add;
            }
        }
    }
}
