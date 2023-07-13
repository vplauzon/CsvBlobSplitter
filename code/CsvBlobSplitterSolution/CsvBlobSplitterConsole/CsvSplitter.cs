namespace ConsoleApp2
{
    internal class CsvSplitter
    {
        private readonly string _sourceRoot;
        private readonly string _suffix;
        private readonly string _targetRoot;

        public CsvSplitter(string sourceRoot, string suffix, string targetRoot)
        {
            _sourceRoot = sourceRoot;
            _suffix = suffix;
            _targetRoot = targetRoot;
        }

        public Task SplitAsync()
        {
            throw new NotImplementedException();
        }
    }
}