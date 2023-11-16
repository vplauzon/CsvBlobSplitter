using KustoBlobSplitLib.LineBased;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.Text
{
    internal class BufferFragment : IEnumerable<byte>
    {
        private readonly byte[] _buffer;
        private readonly int _offset;

        #region Constructors
        private BufferFragment(
            byte[] buffer,
            int offset,
            int length)
        {
            _buffer = buffer;
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

        #region Merge
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
                if (!object.ReferenceEquals(_buffer, other._buffer))
                {
                    throw new ArgumentException(nameof(other), "Not related to same buffer");
                }

                var left = _offset < other._offset ? this : other;
                var right = _offset > other._offset ? other : this;

                if (left._offset + left.Length % _buffer.Length != right._offset)
                {
                    throw new ArgumentException(nameof(other), "Not contiguous");
                }

                return new BufferFragment(_buffer, left._offset, left.Length + right.Length);
            }
        }

        public (BufferFragment, IImmutableList<BufferFragment>) TryMerge(
            IEnumerable<BufferFragment> others)
        {
            var sortedOthers = others
                .OrderBy(f => f._offset < _offset ? f._offset + _buffer.Length : f._offset);
            var stack = new Stack<BufferFragment>(sortedOthers);
            var mergedFragment = this;

            while (stack.Any())
            {
                var other = stack.Peek();

                if (mergedFragment._offset + mergedFragment.Length == other._offset)
                {
                    mergedFragment = mergedFragment.Merge(other);
                    stack.Pop();
                }
                else
                {
                    break;
                }
            }

            return (mergedFragment, stack.ToImmutableArray());
        }
        #endregion

        #region Splice
        /// <summary>This includes the specified index and everything before.</summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public BufferFragment SpliceBefore(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return new BufferFragment(_buffer, _offset, index + 1);
        }

        /// <summary>This excludes the specified index and includes everything after.</summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public BufferFragment SpliceAfter(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            if (index == Length - 1)
            {
                return Empty;
            }
            else
            {
                return new BufferFragment(_buffer, _offset + index + 1, Length - index - 1);
            }
        }
        #endregion

        #region IEnumerable<byte>
        IEnumerator<byte> IEnumerable<byte>.GetEnumerator()
        {
            var end = _offset + Length;

            for (int i = _offset; i != end; ++i)
            {
                yield return _buffer[i % _buffer.Length];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<byte>)this).GetEnumerator();
        }
        #endregion
    }
}