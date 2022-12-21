using Serilog;
using Serilog.Events;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator {
    internal class Program {
        static void Main(string[] args) {
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

            MyConsole.Information("Reading configuration at " + Misc.FILEPATH_CONFIG);
            AppConfig config = null;

            try {
                using(StreamReader r = new StreamReader(Misc.FILEPATH_CONFIG)) {
                    string json = r.ReadToEnd();
                    config = JsonSerializer.Deserialize<AppConfig>(json);
                }
                MyConsole.Information("Configuration loaded : " + JsonSerializer.Serialize(config) + "\n");
            } catch(Exception e) {
                MyConsole.Error(e, "Error upon loading config file");
                return;
            }
            GlobalConfig.loadConfig(config);

            List<DbConnection_> connList = new List<DbConnection_>();

            foreach(DbLoginInfo loginInfo in config.databases) {
                connList.Add(new DbConnection_(loginInfo));
            }

            DbConnection_[] connections = connList.ToArray();

            try {
                if(config.pre_queries_path != null) {
                    QueryExecutor qe = new QueryExecutor(connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault());
                    qe.execute(config.pre_queries_path);
                }

                IdRemapper.loadMap();
                {
                    { //pre-requirement for AspNetUsers
                        {
                            new MasterOccupation(connections).run();
                            {
                                {
                                    new MasterModule(connections).run();
                                }
                                new MasterModuleGroup(connections).run();
                            }
                            //new RelationModule_ModuleGroup(connections).run(); //using query instead
                        }
                        //new AspNetUsers(connections).run(); //using query instead
                    }
                }

                {
                    { //master_faction
                        {
                            new MasterFaction(connections).run();
                            new MasterZone(connections).run();
                        }
                        new MasterFactionZoneRate(connections).run();
                        new RelationFactionPosition(connections).run();
                    }
                }

                {
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
                    new MasterAccountRelation(connections).run();
                    new MasterAccountGLSign(connections).run();
                }

                {
                    {
                        {
                            new MasterGLReport(connections).run();
                        }
                        new MasterGLReportDetail(connections).run();
                    }
                    new MasterGLReportSubDetail(connections).run();
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
                            new TransactionProgramBudget(connections).run();
                        }
                        new TransactionBudget(connections).run();
                    }
                    new TransactionJournal(connections).run();
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
                        new TransactionBudgetDetail(connections).run();
                    }
                    new TransactionJournalDetail(connections).run();
                }

                new TransactionJournalReval(connections).run();
                new TransactionJournalSaldo(connections).run();

                {
                    {
                        new MasterInvoiceFormat(connections).run();
                        new MasterInvoiceType(connections).run();
                    }
                    new TransactionSalesOrder(connections).run();
                }

                if(config.post_queries_path != null) {
                    QueryExecutor qe = new QueryExecutor(connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault());
                    qe.execute(config.post_queries_path);
                }
            } catch(Exception e) {
                MyConsole.Error("Program stopped abnormally due to some error");
            } finally { 
                IdRemapper.saveMap();
                foreach(DbConnection_ con in connections) {
                    con.Close();
                }
            }

            stopwatch.Stop();
            MyConsole.Information("Program finished in " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds));

            Console.WriteLine("\n\nPress any key to exit ...");
            Console.ReadLine();
        }
    }
}
