using KustoBlobSplitLib;
using Microsoft.Extensions.Hosting;

namespace KustoBlobSplitServer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            var app = builder.Build();

            // Configure the HTTP request pipeline.

            app.MapGet("/", (HttpContext httpContext) =>
            {
                return string.Empty;
            });

            var webServerTask = app.RunAsync();

            await EtlRun.RunEtlAsync();

            //  Stop web server
            await Task.WhenAll(app.StopAsync(), webServerTask);
        }
    }
}