namespace KustoBlobSplitServer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            var app = builder.Build();

            // Configure the HTTP request pipeline.

            app.MapGet("/", (HttpContext httpContext) =>
            {
                return string.Empty;
            });

            app.Run();
        }
    }
}