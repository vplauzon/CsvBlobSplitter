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
        private readonly bool _propagateHeader;

        public TextLineParsingSink(ITextSink nextSink, bool propagateHeader)
        {
            _nextSink = nextSink;
            _propagateHeader = propagateHeader;
        }

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
            var outputFragment = TextFragment.Empty;
            var sinkTask = _propagateHeader
                ? null
                : _nextSink.ProcessAsync(null, outputFragmentQueue, releaseQueue);

            while (true)
            {
                var inputResult = await inputFragmentQueue.DequeueAsync();

                if (inputResult.IsCompleted)
                {
                    //  Push what is left (in case no \n at the end of line)
                    PushFragment(outputFragmentQueue, outputFragment);
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
                    var lastPushedIndex = -1;
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
                            if (i + outputFragment.Count() > SINK_BUFFER_SIZE
                                || sinkTask == null)
                            {
                                outputFragment = outputFragment.Merge(fragmentBlock
                                    .SpliceBefore(i)
                                    .SpliceAfter(lastPushedIndex)
                                    .ToTextFragment());
                                if (sinkTask == null)
                                {
                                    //  Init sink task with header
                                    sinkTask = _nextSink.ProcessAsync(
                                        outputFragment.FragmentBytes.ToArray().ToTextFragment(),
                                        outputFragmentQueue,
                                        releaseQueue);
                                    releaseQueue.Enqueue(outputFragment.Count());
                                }
                                else
                                {
                                    PushFragment(outputFragmentQueue, outputFragment);
                                }
                                outputFragment = TextFragment.Empty;
                                lastPushedIndex = i;
                            }
                        }
                        ++i;
                    }
                    if (lastPushedIndex < fragmentBlock.Count() - 1)
                    {   //  Keep fragment
                        outputFragment = outputFragment.Merge(
                            fragmentBlock.SpliceAfter(lastPushedIndex).ToTextFragment());
                    }
                }
            }
        }

        private void PushFragment(
            IWaitingQueue<TextFragment> outputFragmentQueue,
            TextFragment fragment)
        {
            if (fragment.Any())
            {
                if (fragment.FragmentBytes == null)
                {
                    throw new ArgumentNullException(nameof(fragment.FragmentBytes));
                }

                outputFragmentQueue.Enqueue(fragment);
            }
        }
    }
}