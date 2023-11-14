using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Compression;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal abstract class TextStreamSinkBase : ITextSink
    {
        private readonly long _maxBytesPerShard;

        public TextStreamSinkBase(
            BlobCompression compression,
            int maxMbPerShard,
            int shardIndex)
        {
            Compression = compression;
            _maxBytesPerShard = ((long)maxMbPerShard) * 1024 * 1024;
            ShardIndex = shardIndex;
        }

        protected BlobCompression Compression { get; }

        protected int ShardIndex { get; }

        async Task ITextSink.ProcessAsync(
            TextFragment? headerFragment,
            IWaitingQueue<TextFragment> fragmentQueue,
            IWaitingQueue<int> releaseQueue)
        {
            var copyBuffer = new byte[0];
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            //  We pre-fetch a fragment not to create an empty blob
            var fragmentResult = await fragmentQueue.DequeueAsync();

            if (!fragmentResult.IsCompleted)
            {
                var fragment = fragmentResult.Item!;

                await using (var blobStream = await CreateOutputStreamAsync())
                await using (var compressedStream = CompressedStream(blobStream))
                await using (var countingStream = new ByteCountingStream(compressedStream))
                {
                    if (headerFragment != null)
                    {
                        copyBuffer = await WriteFragmentAsync(
                            copyBuffer,
                            headerFragment,
                            countingStream);
                    }
                    do
                    {
                        copyBuffer = await WriteFragmentAsync(
                            copyBuffer,
                            fragment,
                            countingStream);
                        releaseQueue.Enqueue(fragment.Count());
                    }
                    while (countingStream.Position < _maxBytesPerShard
                    && !(fragmentResult = await fragmentQueue.DequeueAsync()).IsCompleted);
                }
                await PostWriteAsync();
                Console.WriteLine($"Sealed shard {ShardIndex} ({stopwatch.Elapsed})");
            }
        }

        protected abstract Task<Stream> CreateOutputStreamAsync();
        
        protected abstract Task PostWriteAsync();

        private async Task<byte[]> WriteFragmentAsync(
            byte[] copyBuffer,
            TextFragment fragment,
            Stream stream)
        {
            if (fragment.FragmentBlock != null)
            {
                await stream.WriteAsync(
                    fragment.FragmentBlock.Buffer,
                    fragment.FragmentBlock.Offset,
                    fragment.FragmentBlock.Count);
            }
            else
            {
                copyBuffer = CopyBytes(copyBuffer, fragment.FragmentBytes);
                await stream.WriteAsync(
                    copyBuffer,
                    0,
                    fragment.FragmentBytes.Count());
            }

            return copyBuffer;
        }

        private byte[] CopyBytes(byte[] copyBuffer, IEnumerable<byte> fragmentBytes)
        {
            var i = 0;

            if (copyBuffer.Length < fragmentBytes.Count())
            {
                copyBuffer = new byte[fragmentBytes.Count()];
            }
            foreach (var b in fragmentBytes)
            {
                copyBuffer[i++] = b;
            }

            return copyBuffer;
        }

        private Stream CompressedStream(Stream stream)
        {
            switch (Compression)
            {
                case BlobCompression.None:
                    return stream;
                case BlobCompression.Gzip:
                    return new GZipStream(stream, CompressionMode.Compress);

                default:
                    throw new NotSupportedException(Compression.ToString());
            }
        }
    }
}