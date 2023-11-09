using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal class LineBasedFragment
    {
        private readonly TaskCompletionSource _releaseSource = new();

        public LineBasedFragment(
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

        public Task ReleasedTask => _releaseSource.Task;

        public IEnumerable<byte> FragmentBytes { get; }

        public MemoryBlock? FragmentBlock { get; }

        public void Release()
        {
            _releaseSource.SetResult();
        }
    }
}