using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal interface ILineBasedSink
    {
        bool HasHeaders { get; }

        Task ProcessAsync(WaitingQueue<LineBasedFragment> fragmentQueue);
    }
}