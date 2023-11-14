using Kusto.Data.Common;
using Kusto.Ingest;
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
    internal class TextKustoSink : TextStreamSinkBase
    {
        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly string _kustoDb;
        private readonly string _kustoTable;
        private readonly string _filePath;

        public TextKustoSink(
            IKustoQueuedIngestClient ingestClient,
            string kustoDb,
            string kustoTable,
            string localTempFolder,
            BlobCompression compression,
            int maxMbPerShard,
            int shardIndex)
            : base(compression, maxMbPerShard, shardIndex)
        {
            _ingestClient = ingestClient;
            _kustoDb = kustoDb;
            _kustoTable = kustoTable;
            _filePath = Path.Combine(localTempFolder, $"{ShardIndex}.txt");
        }

        protected override async Task<Stream> CreateOutputStreamAsync()
        {
            await Task.CompletedTask;

            var stream = File.OpenWrite(_filePath);

            return stream;
        }

        protected override async Task PostWriteAsync()
        {
            var stream = File.OpenRead(_filePath);
            var properties = new KustoIngestionProperties(_kustoDb, _kustoTable)
            {
                Format = DataSourceFormat.txt
            };

            await _ingestClient.IngestFromStreamAsync(
                stream,
                properties,
                new StreamSourceOptions
                {
                    CompressionType = GetCompressionType()
                });
        }

        private DataSourceCompressionType GetCompressionType()
        {
            switch (Compression)
            {
                case BlobCompression.None:
                    return DataSourceCompressionType.None;
                case BlobCompression.Gzip:
                    return DataSourceCompressionType.GZip;

                default:
                    throw new NotSupportedException($"{Compression}");
            }
        }
    }
}