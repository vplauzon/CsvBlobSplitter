using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;

namespace CsvBlobSplitterConsole.Csv
{
    internal class CsvBlobSink : ICsvSink
    {
        private const int WRITING_BUFFER_SIZE = 200000000;
        private const int COMPRESSION_BUFFER_SIZE = 1024 * 1024;
        private const int QUEUE_MAX_BUFFER_SIZE = 10 * 1024 * 1024;
        private readonly TimeSpan WAIT_TIME = TimeSpan.FromSeconds(1);

        private readonly BlobContainerClient _destinationBlobContainer;
        private readonly string _destinationBlobPrefix;
        private readonly int _maxRowsPerShard;
        private readonly long _maxBytesPerShard;
        private readonly IEnumerable<string>? _headers;
        private readonly ConcurrentQueue<IEnumerable<string>> _dataQueue = new();
        private volatile int _queueSize = 0;
        private volatile bool _isCompleted = false;
        private Task? _processTask = null;

        public CsvBlobSink(
            BlobContainerClient destinationBlobContainer,
            string destinationBlobPrefix,
            int maxRowsPerShard,
            int maxMbPerShard,
            IEnumerable<string>? headers)
        {
            _destinationBlobContainer = destinationBlobContainer;
            _destinationBlobPrefix = destinationBlobPrefix;
            _headers = headers;
            _maxRowsPerShard = maxRowsPerShard;
            _maxBytesPerShard = ((long)maxMbPerShard) * 1024 * 1024;
        }

        void ICsvSink.Start()
        {
            if (_processTask != null)
            {
                throw new NotSupportedException("Process already started");
            }
            _processTask = Task.Run(() => ProcessRowsAsync());
        }

        async Task ICsvSink.PushRowAsync(IEnumerable<string> row)
        {
            if (_processTask == null)
            {
                throw new NotSupportedException("Process hasn't started");
            }
            Interlocked.Add(ref _queueSize, row.Sum(i => i.Length));

            //  Wait until queue gets within capacity
            while (_queueSize > QUEUE_MAX_BUFFER_SIZE)
            {
                await Task.Delay(WAIT_TIME);
            }
            _dataQueue.Enqueue(row);
        }

        async Task ICsvSink.CompleteAsync()
        {
            if (_processTask == null)
            {
                throw new NotSupportedException("Process hasn't started");
            }
            _isCompleted = true;
            await _processTask;
            _processTask = null;
        }

        private async Task ProcessRowsAsync()
        {
            var shardCounter = 1;

            while (!_isCompleted)
            {
                await ProcessShardAsync(shardCounter);
                ++shardCounter;
            }
        }

        private async Task ProcessShardAsync(int shardCounter)
        {
            var stopWatch = new Stopwatch();
            var writeOptions = new BlobOpenWriteOptions
            {
                BufferSize = WRITING_BUFFER_SIZE
            };
            var shardName = $"{_destinationBlobPrefix}-{shardCounter}.csv.gz";
            var shardBlobClient = _destinationBlobContainer.GetBlobClient(shardName);
            var rowCount = 0;

            stopWatch.Start();
            using (var blobStream = await shardBlobClient.OpenWriteAsync(true, writeOptions))
            using (var gzipStream = new GZipStream(blobStream, CompressionLevel.Fastest))
            using (var bufferedStream = new BufferedStream(gzipStream, COMPRESSION_BUFFER_SIZE))
            using (var countingStream = new ByteCountingStream(bufferedStream))
            using (var textWriter = new StreamWriter(countingStream))
            using (var csvWriter = new CsvWriter(textWriter, CultureInfo.InvariantCulture))
            {
                if (_headers != null)
                {
                    await WriteRowAsync(csvWriter, _headers);
                }
                while (!_isCompleted
                    && rowCount < _maxRowsPerShard
                    && countingStream.Position < _maxBytesPerShard)
                {
                    if (_dataQueue.TryDequeue(out var row))
                    {
                        await WriteRowAsync(csvWriter, row);
                        ++rowCount;
                        Interlocked.Add(ref _queueSize, -row.Sum(i => i.Length));
                    }
                    else
                    {
                        await Task.Delay(WAIT_TIME);
                    }
                }
                Console.WriteLine(
                    $"Sealing shard '{shardName}' with {rowCount} rows & "
                    + $"{countingStream.Position/1024/1024} Mb in {stopWatch.Elapsed}");
            }
        }

        private static async Task WriteRowAsync(CsvWriter csvWriter, IEnumerable<string> row)
        {
            foreach (var field in row)
            {
                csvWriter.WriteField(field);
            }
            await csvWriter.NextRecordAsync();
        }
    }
}