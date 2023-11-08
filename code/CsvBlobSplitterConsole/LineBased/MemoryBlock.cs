using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal record MemoryBlock(byte[] Buffer, int Offset, int Length) : IEnumerable<byte>
    {
        public int Count => Length;

        IEnumerator<byte> IEnumerable<byte>.GetEnumerator()
        {
            var end = Offset + Length;

            for (int i = Offset; i != end; ++i)
            {
                yield return Buffer[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<byte>)this).GetEnumerator();
        }
    }
}