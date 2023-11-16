using Kusto.Data.Common;
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
        protected const int WRITING_BUFFER_SIZE = 20 * 1024 * 1024;

        public TextStreamSinkBase(RunningContext context, int shardIndex)
        {
            Context = context;
            ShardIndex = shardIndex;
        }

        protected RunningContext Context { get; }

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
                    while (countingStream.Position < Context.BlobSettings.MaxBytesPerShard
                    && !(fragmentResult = await fragmentQueue.DequeueAsync()).IsCompleted);
                }
                await PostWriteAsync();
                Console.WriteLine($"Sealed shard {ShardIndex} ({stopwatch.Elapsed})");
            }
        }

        protected abstract Task<Stream> CreateOutputStreamAsync();
        
        protected abstract Task PostWriteAsync();

        protected string GetCompressionExtension()
        {
            switch (Context.BlobSettings.OutputCompression)
            {
                case DataSourceCompressionType.None:
                    return string.Empty;
                case DataSourceCompressionType.GZip:
                    return ".gz";

                default:
                    throw new NotSupportedException(
                        Context.BlobSettings.OutputCompression.ToString());
            }
        }

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
            switch (Context.BlobSettings.OutputCompression)
            {
                case DataSourceCompressionType.None:
                    return stream;
                case DataSourceCompressionType.GZip:
                    return new GZipStream(stream, CompressionMode.Compress);

                default:
                    throw new NotSupportedException(
                        Context.BlobSettings.OutputCompression.ToString());
            }
        }
    }
}