using Serilog;
using Serilog.Events;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator {
    // transaksi_jurnalsaldo => transaction_journal_saldo
    // transaksi_jurnalkursreval => transaction_journal_reval
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
            Log.Logger.Information("Configuration loaded : " + JsonSerializer.Serialize(config) + "\n");

            DbConnection_[] connections = new DbConnection_[] { 
                new DbConnection_(config.source),
                new DbConnection_(config.destination)
            };

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
                    //new TransactionJournal(connections).run(true, 2183);
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
                }
                
            } catch(Exception) {
                Log.Logger.Error("Program stopped abnormally due to some error");
            }

            Console.WriteLine("\n\nPress any key to exit ...");
            Console.ReadLine();
        }
    }
}
