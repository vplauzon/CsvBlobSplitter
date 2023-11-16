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

        #region Constructors
        private BufferFragment(
            byte[] data,
            int offset,
            int length)
        {
            _data = data;
            _offset = offset;
            Length = length;
        }

        public static BufferFragment Create(int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length), length.ToString());
            }
            else if (length == 0)
            {
                return Empty;
            }
            else
            {
                return new BufferFragment(new byte[length], 0, length);
            }
        }
        #endregion

        public static BufferFragment Empty { get; } = new BufferFragment(new byte[0], 0, 0);

        public int Length { get; }

        public bool Any() => Length > 0;

        public BufferFragment Merge(BufferFragment other)
        {
            if (Length == 0)
            {
                return other;
            }
            else if (other.Length == 0)
            {
                return this;
            }
            else
            {
                if (!object.ReferenceEquals(_data, other._data))
                {
                    throw new ArgumentException(nameof(other), "Not related to same buffer");
                }

                var left = _offset < other._offset ? this : other;
                var right = _offset > other._offset ? other : this;

                if (left._offset + left.Length % _data.Length != right._offset)
                {
                    throw new ArgumentException(nameof(other), "Not contiguous");
                }

                return new BufferFragment(_data, left._offset, left.Length + right.Length);
            }
        }

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