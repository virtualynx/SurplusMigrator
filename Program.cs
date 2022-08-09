using Serilog;
using Serilog.Events;
using SurplusMigrator.Models;
using System;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator {
    internal class Program {
        static void Main(string[] args) {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.File(
                    "log.txt", 
                    //outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
                    restrictedToMinimumLevel: LogEventLevel.Information,
                    rollingInterval: RollingInterval.Day,
                    rollOnFileSizeLimit: true,
                    fileSizeLimitBytes: 50000000
                )
                .WriteTo.Console(restrictedToMinimumLevel: LogEventLevel.Information)
                .CreateLogger();

            Log.Logger.Information("Reading configuration at " + System.Environment.CurrentDirectory + @"\config.json");
            DbConfig config = null;
            using(StreamReader r = new StreamReader(System.Environment.CurrentDirectory + @"\config.json")) {
                string json = r.ReadToEnd();
                config = JsonSerializer.Deserialize<DbConfig>(json);
            }
            Log.Logger.Information("Configuration loaded : " + JsonSerializer.Serialize(config));

            DbConnection_[] connections = new DbConnection_[] { 
                new DbConnection_(config.source),
                new DbConnection_(config.destination)
            };

            //pre-req for MasterAccount
            new MasterAccountReport(connections).run();
            new MasterAccountGroup(connections).run();
            new MasterAccountSubGroup(connections).run();
            new MasterAccountSubType(connections).run();
            new MasterAccountType(connections).run();
            //master_account
            new MasterAccount(connections).run();

            //pre-req for TransactionJournal
            new MasterAccountCa(connections).run();
            new MasterCurrency(connections).run();

            Log.Logger.Information("\n\nPress any key to continue ...");
            Console.ReadLine();
        }
    }
}
