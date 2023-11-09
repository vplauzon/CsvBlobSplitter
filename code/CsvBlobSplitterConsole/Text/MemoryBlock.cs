using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal record MemoryBlock(byte[] Buffer, int Offset, int Length) : ICollection<byte>
    {
        public int Count => Length;

        bool ICollection<byte>.IsReadOnly => true;

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

        void ICollection<byte>.CopyTo(byte[] array, int arrayIndex)
        {
            Array.Copy(Buffer, Offset, array, arrayIndex, Length);
        }

        void ICollection<byte>.Add(byte item)
        {
            throw new NotSupportedException();
        }

        void ICollection<byte>.Clear()
        {
            throw new NotSupportedException();
        }

        bool ICollection<byte>.Contains(byte item)
        {
            throw new NotSupportedException();
        }

        bool ICollection<byte>.Remove(byte item)
        {
            throw new NotSupportedException();
        }
    }
}