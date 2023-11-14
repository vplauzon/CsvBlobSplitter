using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal static class TextFragmentHelper
    {
        public static TextFragment ToTextFragment(this IEnumerable<byte> fragmentBytes)
        {
            var fragment = fragmentBytes is MemoryBlock block
                ? new TextFragment(block, block)
                : new TextFragment(fragmentBytes, null);

            return fragment;
        }
    }
}