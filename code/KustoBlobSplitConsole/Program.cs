using System.Diagnostics;
using KustoBlobSplitLib;

namespace KustoBlobSplitConsole
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();

            await EtlRun.RunEtlAsync(runSettings);
        }
    }
}