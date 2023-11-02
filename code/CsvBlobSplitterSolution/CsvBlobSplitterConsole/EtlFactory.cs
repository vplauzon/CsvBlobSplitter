using CsvBlobSplitterConsole.Csv;

namespace CsvBlobSplitterConsole
{
    internal class EtlFactory
    {
        public static IEtl Create(RunSettings runSettings)
        {
            var source = new CsvBlobSource(runSettings.SourceBlob, runSettings.Compression);

            return new CsvEtl(
                source,
                (headers) => new CsvBlobSplit(runSettings.DestinationBlobPrefix!, headers),
                runSettings.HasCsvHeaders);
        }
    }
}