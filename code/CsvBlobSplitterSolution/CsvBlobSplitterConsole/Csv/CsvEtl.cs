using System;
using System.Collections.Generic;
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
            var isFirstRow = true;
            ICsvSink? sink = null;

            await foreach (var row in _source.RetrieveRowsAsync())
            {
                if (isFirstRow)
                {
                    isFirstRow = false;
                    if (_hasCsvHeaders)
                    {   //  Use first row as headers
                        sink = _sinkFactory(row);
                    }
                    else
                    {
                        sink = _sinkFactory(null);
                        await sink!.PushRowAsync(row);
                    }
                }
                else
                {
                    await sink!.PushRowAsync(row);
                }
            }
            //  If blob is empty, the sink will be null
            if (sink != null)
            {
                await sink!.CompleteAsync();
            }
        }
    }
}