using KustoBlobSplitLib.LineBased;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Kusto.Cloud.Platform.Utils.CachedBufferEncoder;

namespace KustoBlobSplitLib.Text
{
    internal class TextLineParsingSink : ITextSink
    {
        private const int SINK_BUFFER_SIZE = 1024 * 1024;

        private readonly ITextSink _nextSink;

        public TextLineParsingSink(ITextSink nextSink)
        {
            _nextSink = nextSink;
        }

        bool ITextSink.HasHeaders => _nextSink.HasHeaders;

        async Task ITextSink.ProcessAsync(
            TextFragment? headerFragment,
            IWaitingQueue<TextFragment> inputFragmentQueue,
            IWaitingQueue<int> releaseQueue)
        {
            if (headerFragment != null)
            {
                throw new ArgumentOutOfRangeException(nameof(headerFragment));
            }

            var outputFragmentQueue =
                new WaitingQueue<TextFragment>() as IWaitingQueue<TextFragment>;
            var outputFragmentBytes = (IEnumerable<byte>?)null;
            var sinkTask = _nextSink.HasHeaders
                ? null
                : _nextSink.ProcessAsync(null, outputFragmentQueue, releaseQueue);

            while (true)
            {
                var inputResult = await inputFragmentQueue.DequeueAsync();

                if (inputResult.IsCompleted)
                {
                    if (outputFragmentBytes != null)
                    {   //  Push what is left (in case no \n at the end of line)
                        PushFragment(outputFragmentQueue, outputFragmentBytes);
                    }
                    outputFragmentQueue.Complete();
                    if (sinkTask != null)
                    {
                        await sinkTask;
                    }

                    return;
                }
                else
                {
                    var i = 0;
                    var lastPushIndex = 0;
                    var fragmentBlock = inputResult.Item!.FragmentBlock;

                    if (fragmentBlock == null)
                    {
                        throw new NotSupportedException(
                            "FragmentBlock should always be present in this context");
                    }

                    foreach (var b in fragmentBlock)
                    {
                        if (b == '\n')
                        {
                            if (i + (outputFragmentBytes?.Count() ?? 0) > SINK_BUFFER_SIZE
                                || sinkTask == null)
                            {
                                var fragment = MergeFragments(
                                    outputFragmentBytes,
                                    fragmentBlock
                                    .SpliceBefore(i)
                                    .SpliceAfter(lastPushIndex));

                                if (sinkTask == null)
                                {
                                    //  Init sink task with header
                                    sinkTask = _nextSink.ProcessAsync(
                                        null,
                                        outputFragmentQueue,
                                        releaseQueue);
                                }
                                PushFragment(outputFragmentQueue, fragment);
                                outputFragmentBytes = null;
                                lastPushIndex = i;
                            }
                        }
                        ++i;
                    }
                    if (lastPushIndex < fragmentBlock.Count() - 1)
                    {   //  Keep fragment
                        outputFragmentBytes = MergeFragments(
                            outputFragmentBytes,
                            fragmentBlock.SpliceAfter(lastPushIndex));
                    }
                }
            }
        }

        private void PushFragment(
            IWaitingQueue<TextFragment> outputFragmentQueue,
            IEnumerable<byte> outputFragmentBytes)
        {
            var fragment = outputFragmentBytes is MemoryBlock block
                ? new TextFragment(block, block)
                : new TextFragment(outputFragmentBytes, null);

            if (fragment.FragmentBytes == null)
            {
                throw new ArgumentNullException(nameof(fragment.FragmentBytes));
            }

            outputFragmentQueue.Enqueue(fragment);
        }

        private static IEnumerable<byte> MergeFragments(
            IEnumerable<byte>? outputFragmentBytes,
            MemoryBlock fragmentBlock)
        {
            return outputFragmentBytes == null
                ? fragmentBlock
                : outputFragmentBytes.Concat(fragmentBlock);
        }
    }
}