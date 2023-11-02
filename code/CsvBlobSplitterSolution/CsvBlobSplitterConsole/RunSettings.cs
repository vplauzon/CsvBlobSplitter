using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole
{
    internal class RunSettings
    {
        public string Source { get; set; } = string.Empty;

        public string Destination { get; set; } = string.Empty;

        public BlobCompression Compression { get; set; } = BlobCompression.None;

        public bool HasHeaders { get; set; } = true;
    }
}