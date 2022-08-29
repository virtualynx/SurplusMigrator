﻿using Serilog;
using Serilog.Events;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.Json;

namespace SurplusMigrator {
    // transaction_journal          transaksi_jurnal
    // transaction_journal_detail   transaksi_jurnaldetil
    // master_glreport_subdetail    master_gl_report_row_acc
    // master_glreport_detail       master_gl_report_row
    // master_glreport              master_gl_report_row_h

    // transaksi_jurnalkursreval    transaction_journal_reval
    // transaksi_jurnalsaldo        transaction_journal_saldo
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
            using(StreamReader r = new StreamReader(Misc.FILEPATH_CONFIG)) {
                string json = r.ReadToEnd();
                config = JsonSerializer.Deserialize<AppConfig>(json);
            }
            MyConsole.Information("Configuration loaded : " + JsonSerializer.Serialize(config) + "\n");
            GlobalConfig.loadConfig(config);

            List<DbConnection_> connList = new List<DbConnection_>();

            foreach(DbLoginInfo loginInfo in config.databases) {
                connList.Add(new DbConnection_(loginInfo));
            }

            DbConnection_[] connections = connList.ToArray();

            IdRemapper.loadMap();

            try {
                {
                    { //master_account
                        {//pre-req for MasterAccount
                            new MasterAccountReport(connections).run();
                            new MasterAccountGroup(connections).run();
                            new MasterAccountSubGroup(connections).run();
                            new MasterAccountSubType(connections).run();
                            new MasterAccountType(connections).run();
                        }
                        new MasterAccount(connections).run(true);
                    }
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
                        new TransactionBudget(connections).run(true); //
                    }
                    new TransactionJournal(connections).run(true); //
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
                        new TransactionBudgetDetail(connections).run(true); //
                    }
                    new TransactionJournalDetail(connections).run(true); //
                }

                new TransactionJournalReval(connections).run(true); //
            } catch(Exception e) {
                MyConsole.Error(e, "Program stopped abnormally due to some error");
            } finally { 
                IdRemapper.saveMap();
            }

            stopwatch.Stop();
            MyConsole.Information("Program finished in " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds));

            Console.WriteLine("\n\nPress any key to exit ...");
            Console.ReadLine();
        }
    }
}
