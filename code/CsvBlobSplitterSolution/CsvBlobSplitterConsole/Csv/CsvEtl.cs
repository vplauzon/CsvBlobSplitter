using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.Csv
{
    internal class CsvEtl : IEtl
    {
        private readonly ICsvSource _source;
        private readonly Func<IEnumerable<string>?, ICsvSink> _sinkFactory;
        private readonly bool _hasCsvHeaders;

        public CsvEtl(
            ICsvSource source,
            Func<IEnumerable<string>?, ICsvSink> sinkFactory,
            bool hasCsvHeaders)
        {
            _source = source;
            _sinkFactory = sinkFactory;
            _hasCsvHeaders = hasCsvHeaders;
        }

        async Task IEtl.ProcessAsync()
        {
            var stopWatch = new Stopwatch();
            var lastElapsed = stopWatch.Elapsed;
            var isFirstRow = true;
            var rowCount = (long)0;
            ICsvSink? sink = null;

            stopWatch.Start();
            await foreach (var row in _source.RetrieveRowsAsync())
            {
                if (isFirstRow)
                {
                    isFirstRow = false;
                    if (_hasCsvHeaders)
                    {   //  Use first row as headers
                        sink = _sinkFactory(row);
                        sink.Start();
                    }
                    else
                    {
                        sink = _sinkFactory(null);
                        sink.Start();
                        await sink!.PushRowAsync(row);
                        ++rowCount;
                    }
                }
                else
                {
                    await sink!.PushRowAsync(row);
                    ++rowCount;
                }
                if (stopWatch.Elapsed.Seconds / 10 != lastElapsed.Seconds / 10)
                {
                    Console.WriteLine($"ETL:  {rowCount} rows at {stopWatch.Elapsed}");
                }
                lastElapsed = stopWatch.Elapsed;
            }
            //  If blob is empty, the sink will be null
            if (sink != null)
            {
                await sink!.CompleteAsync();
            }
            Console.WriteLine($"ETL:  completed {rowCount} rows at {stopWatch.Elapsed}");
        }
    }
}