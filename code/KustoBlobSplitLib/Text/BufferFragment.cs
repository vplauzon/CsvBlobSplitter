using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.Text
{
    internal class BufferFragment : IEnumerable<byte>
    {
        private readonly byte[] _data;
        private readonly int _offset;

        public BufferFragment(int length)
        {
            _data = new byte[length];
            _offset = 0;
            Length = length;
        }

        public int Length { get; }

        #region IEnumerable<byte>
        IEnumerator<byte> IEnumerable<byte>.GetEnumerator()
        {
            var end = _offset + Length;

            for (int i = _offset; i != end; ++i)
            {
                yield return _data[i % _data.Length];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<byte>)this).GetEnumerator();
        }
        #endregion
    }
}