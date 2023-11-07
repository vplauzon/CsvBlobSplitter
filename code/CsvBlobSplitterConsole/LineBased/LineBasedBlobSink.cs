using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal class LineBasedBlobSink
    {
        private readonly BlobContainerClient _destinationBlobContainer;
        private readonly string _destinationBlobPrefix;
        private readonly int _maxRowsPerShard;
        private readonly long _maxBytesPerShard;

        public LineBasedBlobSink(
            BlobContainerClient destinationBlobContainer,
            string destinationBlobPrefix,
            int maxRowsPerShard,
            int maxMbPerShard)
        {
            _destinationBlobContainer = destinationBlobContainer;
            _destinationBlobPrefix = destinationBlobPrefix;
            _maxRowsPerShard = maxRowsPerShard;
            _maxBytesPerShard = ((long)maxMbPerShard) * 1024 * 1024;
        }
    }
}