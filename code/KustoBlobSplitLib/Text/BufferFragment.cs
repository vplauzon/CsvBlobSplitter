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
        #region Inner Types

        //public record MemoryBlock(byte[] Buffer, int Offset, int Length);
        #endregion

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

        public IEnumerable<Memory<byte>> GetMemoryBlocks()
        {
            if (_offset + Length <= _buffer.Length)
            {
                yield return new Memory<byte>(_buffer, _offset, Length);
            }
            else
            {
                yield return new Memory<byte>(_buffer, _offset, _buffer.Length - _offset);
                yield return new Memory<byte>(_buffer, 0, Length - (_buffer.Length - _offset));
            }
        }

        public override string ToString()
        {
            return $"({_offset}, {_offset + Length}):  Length = {Length}";
        }

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
                var right = _offset < other._offset ? other : this;

                if (left._offset == 0 && right._offset + Length == _buffer.Length)
                {
                    return new BufferFragment(_buffer, right._offset, right.Length + left.Length);
                }
                else if ((left._offset + left.Length) % _buffer.Length != right._offset)
                {
                    throw new ArgumentException(nameof(other), "Not contiguous");
                }
                else
                {
                    return new BufferFragment(_buffer, left._offset, left.Length + right.Length);
                }
            }
        }

        public (BufferFragment Fragment, IImmutableList<BufferFragment> List) TryMerge(
            IEnumerable<BufferFragment> others)
        {
            var sortedOthers = others
                .OrderBy(f => f._offset < _offset ? f._offset + _buffer.Length : f._offset);
            var stack = new Stack<BufferFragment>(sortedOthers);
            var mergedFragment = this;

            while (stack.Any())
            {
                var other = stack.Peek();
                var left = _offset < other._offset ? this : other;
                var right = _offset < other._offset ? other : this;

                if (left._offset == 0 && right._offset + Length == _buffer.Length
                    || left._offset + left.Length == right._offset)
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

            return new BufferFragment(_buffer, _offset, index);
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