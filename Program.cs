// See https://aka.ms/new-console-template for more information
using Castle.Core.Logging;
using IT_Core_SyncDB;
using IT_Core_SyncDB.Interface;
using IT_Core_SyncDB.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NLog;
using NLog.Extensions.Logging;
using NLog.Web;



var Configuration = new ConfigurationBuilder().SetBasePath($"{Directory.GetCurrentDirectory()}").AddJsonFile("appsettings.json", optional: true, reloadOnChange: true).Build();


NLog.LogManager.Configuration = new NLogLoggingConfiguration(Configuration.GetSection("NLog"));
Logger logger = LogManager.GetCurrentClassLogger();


try
{
    var IHost = Host.CreateDefaultBuilder().ConfigureServices((context, services) =>
    {
        services.AddSingleton<IMain, Application>();
        services.AddSingleton<IDBImport, DBImport>();
        services.AddScoped<IConfiguration>(provider => Configuration);


        //DI DB Connections
        var DBConnectionsSection = Configuration.GetSection("ConnectionStrings");
        foreach (var DBSection in DBConnectionsSection.GetChildren())
        {
            string? ConnectionString = DBSection.Value;
            string DBName = DBSection.Key;

            if (string.IsNullOrEmpty(ConnectionString) || string.IsNullOrEmpty(DBName)) continue;

            services.AddTransient<IDB>(provider => new DBHandle(ConnectionString, DBName));
        }
    })
    .UseNLog()
    .Build();

    var app = IHost.Services.GetRequiredService<IMain>();
    app.Run();
}
catch (System.Exception ex)
{
    logger.Error($"App Start Error: {ex.Message}. From \n{ex.Source}.");
}



