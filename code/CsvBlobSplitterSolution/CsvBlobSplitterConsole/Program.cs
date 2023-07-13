namespace CsvBlobSplitterConsole
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine(
                    "Usage:  <Source Root Blob Url> <suffix> <Destination Root Blob Url>");
            }
            else
            {
                var sourceRoot = args[0];
                var suffix = args[1];
                var targetRoot = args[2];

                Console.WriteLine($"Source Root Blob Url:  {sourceRoot}");
                Console.WriteLine($"Suffix:  {suffix}");
                Console.WriteLine($"Target Root Blob Url:  {targetRoot}");
                Console.WriteLine();

                var splitter = new CsvSplitter(new Uri(sourceRoot), suffix, new Uri(targetRoot));

                await splitter.SplitAsync();
            }
        }
    }
}