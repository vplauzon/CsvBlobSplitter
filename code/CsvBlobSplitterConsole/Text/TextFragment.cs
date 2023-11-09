using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal class TextFragment
    {
        private readonly TaskCompletionSource _releaseSource = new();

        public TextFragment(
            IEnumerable<byte> fragmentBytes,
            MemoryBlock? fragmentBlock)
        {
            if (fragmentBlock != null)
            {
                if (fragmentBlock.Offset + fragmentBlock.Count > fragmentBlock.Buffer.Length)
                {
                    throw new ArgumentOutOfRangeException(nameof(fragmentBlock));
                }
            }
            FragmentBytes = fragmentBytes;
            FragmentBlock = fragmentBlock;
        }

        public IEnumerable<byte> FragmentBytes { get; }

        public MemoryBlock? FragmentBlock { get; }
    }
}