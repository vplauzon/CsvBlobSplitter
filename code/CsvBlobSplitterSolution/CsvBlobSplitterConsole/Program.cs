namespace CsvBlobSplitterConsole
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine(
                    "Usage:  <Source Root Blob Url> <suffix> <Destination Root Blob Url> <Has Headers:boolean>");
            }
            else
            {
                var sourceRoot = args[0];
                var suffix = args[1];
                var targetRoot = args[2];
                var hasHeaders = args[3];

                Console.WriteLine($"Source Root Blob Url:  {sourceRoot}");
                Console.WriteLine($"Suffix:  {suffix}");
                Console.WriteLine($"Target Root Blob Url:  {targetRoot}");
                Console.WriteLine($"Has headers:  {hasHeaders}");
                Console.WriteLine();

                try
                {
                    var hasHeadersBool = bool.Parse(hasHeaders);

                    var splitter = new CsvSplitter(
                        new Uri(sourceRoot),
                        suffix,
                        new Uri(targetRoot),
                        hasHeadersBool);

                    await splitter.SplitAsync();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{ex.Message}");
                }
            }
        }
    }
}