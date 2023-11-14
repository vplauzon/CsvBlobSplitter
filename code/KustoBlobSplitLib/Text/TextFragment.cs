using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal class TextFragment
    {
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

        /// <summary>For debugging purposes.</summary>
        /// <returns></returns>
        public override string ToString()
        {
            return new string(FragmentBytes.Select(b => (char)b).ToArray());
        }
    }
}