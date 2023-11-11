using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal record MemoryBlock(byte[] Buffer, int Offset, int Length) : ICollection<byte>
    {
        public int Count => Length;

        /// <summary>This includes the specified index and everything before.</summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public MemoryBlock SpliceBefore(int index)
        {
            return new MemoryBlock(Buffer, Offset, index + 1);
        }

        /// <summary>This excludes the specified index and includes everything after.</summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public MemoryBlock SpliceAfter(int index)
        {
            return new MemoryBlock(Buffer, Offset + index + 1, Length - index - 1);
        }

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