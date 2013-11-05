using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.Memcache
{
    public interface IOngoingDispose
    {
        bool OngoingDispose { get; }
    }
}
