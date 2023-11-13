using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal class TextSplitSink : ITextSink
    {
        private readonly bool _hasHeaders;
        private readonly Func<int, ITextSink> _sinkFactory;

        public TextSplitSink(bool hasHeaders, Func<int, ITextSink> sinkFactory)
        {
            _hasHeaders = hasHeaders;
            _sinkFactory = sinkFactory;
        }

        bool ITextSink.HasHeaders => _hasHeaders;

        async Task ITextSink.ProcessAsync(
            WaitingQueue<TextFragment> fragmentQueue,
            WaitingQueue<int> releaseQueue)
        {
            var header = _hasHeaders
                ? await DequeueHeaderAsync(fragmentQueue, releaseQueue)
                : null;
            var subSinkTasks = new List<Task>();
            var subQueues = new List<WaitingQueue<TextFragment>>();

            while (true)
            {
                var fragmentResult = await fragmentQueue.DequeueAsync();

                if (!fragmentResult.IsCompleted)
                {
                    QueueFragment(fragmentResult.Item!, subSinkTasks, subQueues);
                    CleanSubSinks(subSinkTasks, subQueues);
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        private void QueueFragment(
            TextFragment textFragment,
            List<Task> subSinkTasks,
            List<WaitingQueue<TextFragment>> subQueues)
        {
            foreach(var subQueue in subQueues)
            {
            }
            throw new NotImplementedException();
        }

        private void CleanSubSinks(
            List<Task> subSinkTasks,
            List<WaitingQueue<TextFragment>> subQueues)
        {
            throw new NotImplementedException();
        }

        private async Task<byte[]?> DequeueHeaderAsync(
            WaitingQueue<TextFragment> fragmentQueue,
            WaitingQueue<int> releaseQueue)
        {
            var fragmentResult = await fragmentQueue.DequeueAsync();

            if (!fragmentResult.IsCompleted)
            {
                var header = fragmentResult.Item!.FragmentBytes.ToArray();

                releaseQueue.Enqueue(header.Length);

                return header;
            }
            else
            {
                return null;
            }
        }
    }
}