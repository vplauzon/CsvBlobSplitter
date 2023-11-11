using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal interface ITextSink
    {
        bool HasHeaders { get; }

        Task ProcessAsync(
            WaitingQueue<TextFragment> fragmentQueue,
            WaitingQueue<int> releaseQueue);
    }
}