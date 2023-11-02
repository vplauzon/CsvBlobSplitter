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
            var headers = _hasCsvHeaders
                ? await _source.RetrieveRowAsync()
                : null;

            if (_hasCsvHeaders && headers == null)
            {   //  Empty blob
                return;
            }
            else
            {
                var sink = _sinkFactory(headers);

                do
                {
                    var row = await _source.RetrieveRowAsync();

                    if (row == null)
                    {   //  Source has completed
                        await sink.CompleteAsync();
             
                        return;
                    }
                    else
                    {
                        await sink.PushRowAsync(row);
                    }
                }
                while (true);
            }
        }
    }
}