using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
        #region Inner types
        private class ThreadSafeCounter
        {
            private volatile int _counter = 0;

            public int GetNextCounter()
            {
                return Interlocked.Increment(ref _counter);
            }
        }
        #endregion

        private readonly Func<int, ITextSink> _sinkFactory;

        public TextSplitSink(Func<int, ITextSink> sinkFactory)
        {
            _sinkFactory = sinkFactory;
        }

        async Task ITextSink.ProcessAsync(
            TextFragment? headerFragment,
            IWaitingQueue<TextFragment> fragmentQueue,
            IWaitingQueue<int> releaseQueue)
        {
            var counter = new ThreadSafeCounter();
            var processingTasks = Enumerable.Range(0, 2 * Environment.ProcessorCount)
                .Select(i => ProcessFragmentsAsync(
                    counter,
                    headerFragment,
                    fragmentQueue,
                    releaseQueue))
                .ToImmutableArray();

            await Task.WhenAll(processingTasks);
        }

        private async Task ProcessFragmentsAsync(
            ThreadSafeCounter counter,
            TextFragment? headerFragment,
            IWaitingQueue<TextFragment> fragmentQueue,
            IWaitingQueue<int> releaseQueue)
        {
            while (!fragmentQueue.HasCompleted)
            {
                var sink = _sinkFactory(counter.GetNextCounter());

                await sink.ProcessAsync(headerFragment, fragmentQueue, releaseQueue);
            }
        }
    }
}