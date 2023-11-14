using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal class TextFragment
    {
        public TextFragment(IEnumerable<byte> fragmentBytes, MemoryBlock? fragmentBlock)
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

        public static TextFragment Empty { get; } = new TextFragment(new byte[0], null);

        public IEnumerable<byte> FragmentBytes { get; }

        public MemoryBlock? FragmentBlock { get; }

        public bool Any()
        {
            return FragmentBlock != null
                ? FragmentBlock.Length > 0
                : FragmentBytes.Any();
        }

        public int Count()
        {
            return FragmentBlock != null
                ? FragmentBlock.Count
                : FragmentBytes.Count();
        }

        public TextFragment Merge(TextFragment other)
        {
            if (!FragmentBytes.Any())
            {
                return other;
            }
            else if (!other.FragmentBytes.Any())
            {
                return this;
            }
            else if (FragmentBlock != null && other.FragmentBlock != null)
            {
                var mergedBlock = FragmentBlock.TryMerge(other.FragmentBlock);

                if (mergedBlock != null)
                {
                    return mergedBlock.ToTextFragment();
                }
            }

            return new TextFragment(FragmentBytes.Concat(other.FragmentBytes), null);
        }

        /// <summary>For debugging purposes.</summary>
        /// <returns></returns>
        public override string ToString()
        {
            return new string(FragmentBytes.Select(b => (char)b).ToArray());
        }
    }
}