using Serilog;
using Serilog.Events;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Models.Others;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator {
    // transaksi_jurnalsaldo => transaction_journal_saldo
    // transaksi_jurnalkursreval => transaction_journal_reval
    internal class Program {
        static void Main(string[] args) {
            List<RemappedId> save = new List<RemappedId>() {
                new RemappedId() {
                    name = "key1",
                    dataType = typeof(long).Name,
                    maps = new Dictionary<string, object>() {
                        { "11111", 99999 },
                        { "22222", 88888 },
                    }
                },
                new RemappedId() {
                    name = "key2",
                    dataType = typeof(string).Name,
                    maps = new Dictionary<string, object>() {
                        { "ID111", "IDN11" },
                        { "ID222", "IDN22" },
                    }
                }
            };
            string filename = "log_test.json";
            string savePath = System.Environment.CurrentDirectory + "\\" + filename;
            File.WriteAllText(savePath, JsonSerializer.Serialize(save));

            IdRemapper.loadMap(savePath);

            long id1 = IdRemapper.get("key1", 11111);
            string id2 = IdRemapper.get("key2", "ID111");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            MyConsole.stopwatch = stopwatch;

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
                //.WriteTo.Console(restrictedToMinimumLevel: LogEventLevel.Information)
                .CreateLogger();

            //Log.Logger.Information("Reading configuration at " + System.Environment.CurrentDirectory + @"\config.json");
            MyConsole.Information("Reading configuration at " + System.Environment.CurrentDirectory + @"\config.json");
            DbConfig config = null;
            using(StreamReader r = new StreamReader(System.Environment.CurrentDirectory + @"\config.json")) {
                string json = r.ReadToEnd();
                config = JsonSerializer.Deserialize<DbConfig>(json);
            }
            //Log.Logger.Information("Configuration loaded : " + JsonSerializer.Serialize(config) + "\n");
            MyConsole.Information("Configuration loaded : " + JsonSerializer.Serialize(config) + "\n");

            List<DbConnection_> connList = new List<DbConnection_>();

            foreach(DbLoginInfo loginInfo in config.databases) {
                connList.Add(new DbConnection_(loginInfo));
            }

            DbConnection_[] connections = connList.ToArray();

            try {
                { //master_account
                    {//pre-req for MasterAccount
                        new MasterAccountReport(connections).run();
                        new MasterAccountGroup(connections).run();
                        new MasterAccountSubGroup(connections).run();
                        new MasterAccountSubType(connections).run();
                        new MasterAccountType(connections).run();
                    }
                    new MasterAccount(connections).run();
                }

                { //start of TransactionJournal 
                    {//--pre-req for TransactionJournal
                        new MasterAccountCa(connections).run();
                        new MasterAdvertiser(connections).run();
                        new MasterAdvertiserBrand(connections).run();
                        new MasterCurrency(connections).run();
                        new MasterPaymentType(connections).run();
                        new MasterPeriod(connections).run();
                        new MasterTransactionTypeGroup(connections).run();
                        new MasterTransactionType(connections).run();
                        new MasterSource(connections).run();
                        new MasterVendorCategory(connections).run();
                        new MasterVendorType(connections).run();
                        new MasterVendor(connections).run();
                        {//---pre-req for TransactionBudget
                            new MasterProdType(connections).run();
                            new MasterProjectType(connections).run();
                            new MasterShowInventoryCategory(connections).run();
                            new MasterShowInventoryDepartment(connections).run();
                            new MasterShowInventoryTimezone(connections).run();
                            new MasterTvProgramType(connections).run();
                            {//----pre-req for TransactionProgramBudget
                                new MasterProgramBudgetContenttype(connections).run();
                                new MasterProgramBudgetType(connections).run();
                            }
                            new TransactionProgramBudget(connections).run(false, 1925);
                        }
                        new TransactionBudget(connections).run(true, 1169);
                    }
                    new TransactionJournal(connections).run(true, 2183);
                }

                { //start of TransactionJournalDetail
                    {//--pre-req for TransactionJournalDetail
                        {//pre-req for MasterBankAccount
                            new MasterBank(connections).run();
                        }
                        new MasterBankAccount(connections).run();
                        new MasterJournalReferenceType(connections).run();
                        {//---pre-req for TransactionBudgetDetail
                            new MasterBudgetAccount(connections).run();
                        }
                        new TransactionBudgetDetail(connections).run(true, 3855);
                    }
                    new TransactionJournalDetail(connections).run(true, 2114);
                }
            } catch(Exception) {
                //Log.Logger.Error("Program stopped abnormally due to some error");
                MyConsole.Error("Program stopped abnormally due to some error");
            }

            stopwatch.Stop();
            //Log.Logger.Information("Program finished in " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds));
            MyConsole.Information("Program finished in " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds));

            Console.WriteLine("\n\nPress any key to exit ...");
            Console.ReadLine();
        }
    }
}
