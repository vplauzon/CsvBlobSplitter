using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal class SingleSourceEtl : IEtl
    {
        Task IEtl.ProcessAsync()
        {
            throw new NotImplementedException();
        }
    }
}
