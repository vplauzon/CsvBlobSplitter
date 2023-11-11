using System.Diagnostics;
using KustoBlobSplitLib;

namespace KustoBlobSplitConsole
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            await EtlRun.RunEtlAsync();
        }
    }
}