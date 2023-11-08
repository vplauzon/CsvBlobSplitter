using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal class LineBasedFragment
    {
        public LineBasedFragment(
            IEnumerable<byte> fragmentBytes,
            MemoryBlock? fragmentBlock)
        {
            FragmentBytes = fragmentBytes;
            FragmentBlock = fragmentBlock;
        }

        public IEnumerable<byte> FragmentBytes { get; }

        public MemoryBlock? FragmentBlock { get; }
    }
}