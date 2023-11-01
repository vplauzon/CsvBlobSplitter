using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole
{
    internal class CommandLineOptions
    {
        [Option('s', "source", Required = true, HelpText = "Source URL:  storage prefix")]
        public string Source { get; set; } = string.Empty;

        [Option('d', "destination", Required = true, HelpText = "Destination URL:  storage prefix")]
        public string Destination { get; set; } = string.Empty;

        [Option('c', "compression", Required = false, HelpText = "Compression mode:  None, gzip, zip")]
        public BlobCompression Compression { get; set; } = BlobCompression.None;

        [Option('h', "headers", Required = false, HelpText = "Has Headers (for CSV)")]
        public bool HasHeaders { get; set; } = true;
    }
}