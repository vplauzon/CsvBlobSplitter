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
            var shardCount = 0;

            while (true)
            {
                var fragmentResult = await fragmentQueue.DequeueAsync();

                if (!fragmentResult.IsCompleted)
                {
                    QueueFragment(
                        fragmentResult.Item!,
                        subSinkTasks,
                        subQueues,
                        releaseQueue,
                        header,
                        ref shardCount);
                    CleanSubSinks(subSinkTasks, subQueues);
                }
                else
                {   //  Signal end to each sub sink
                    foreach (var subQueue in subQueues)
                    {
                        subQueue.Complete();
                    }
                    await Task.WhenAll(subSinkTasks);
                }
            }
        }

        private void QueueFragment(
            TextFragment textFragment,
            List<Task> subSinkTasks,
            List<WaitingQueue<TextFragment>> subQueues,
            WaitingQueue<int> releaseQueue,
            byte[]? header,
            ref int shardCount)
        {
            foreach (var pair in subSinkTasks.Zip(subQueues))
            {
                var subSinkTask = pair.First;
                var subQueue = pair.Second;

                if (!subSinkTask.IsCompleted && subQueue.IsConsumerWaiting)
                {
                    subQueue.Enqueue(textFragment);
                    return;
                }
            }
            //  If we reached this point, no queue was awaiting fragments
            //  so we create a new one
            var newSubQueue = new WaitingQueue<TextFragment>();
            var newSubSink = _sinkFactory(++shardCount);
            var newSubTask = Task.Run(() => newSubSink.ProcessAsync(newSubQueue, releaseQueue));

            subQueues.Add(newSubQueue);
            subSinkTasks.Add(newSubTask);
            //  We finally pass the header and fragment
            if (header != null)
            {
                newSubQueue.Enqueue(new TextFragment(header, null));
            }
            newSubQueue.Enqueue(textFragment);
        }

        private async void CleanSubSinks(
            List<Task> subSinkTasks,
            List<WaitingQueue<TextFragment>> subQueues)
        {
            var indexToRemove = new List<int>();

            for (int i = 0; i != subSinkTasks.Count; ++i)
            {
                if (subSinkTasks[i].IsCompleted)
                {   //  Observe task ending
                    await subSinkTasks[i];
                    indexToRemove.Add(i);
                }
            }
            foreach (var i in indexToRemove.Reverse<int>())
            {
                subSinkTasks.RemoveAt(i);
                subQueues.RemoveAt(i);
            }
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