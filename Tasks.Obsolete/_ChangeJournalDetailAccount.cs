using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _ChangeJournalDetailAccount : _BaseTask {
        private DbConnection_ targetConnection;

        private const int DEFAULT_BATCH_SIZE = 500;

        public _ChangeJournalDetailAccount(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            MyConsole.Information("Account will be changed on table transaction_journal_detail (schema " + targetConnection.GetDbLoginInfo().schema + ")");
            Console.WriteLine();
            Console.Write("Continue performing fix (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            var newAccountMappings = getDataFromExcel();

            newAccountMappings = newAccountMappings.Skip(1).ToArray(); //removes headers

            foreach(var accMap in newAccountMappings) {
                string tjournal_detailid = Utils.obj2str(accMap["tjournal_detailid"]);
                string accountid = Utils.obj2str(accMap["accountid"]);

                try {
                    string queryUpdate = @"
                            update transaction_journal_detail 
                            set accountid = @accountid
                            where tjournal_detailid = @tjournal_detailid;
                        "
                    ;

                    var updateResult = QueryUtils.executeQuery(
                        targetConnection,
                        queryUpdate,
                        new Dictionary<string, object> {
                            { "@tjournal_detailid", tjournal_detailid },
                            { "@accountid", accountid }
                        }
                    );

                    MyConsole.Information(
                        "AccountId on journalDetail [<tjournal_detailid>] has been updated ... "
                        .Replace("<tjournal_detailid>", tjournal_detailid)
                    );
                } catch(Exception) {
                    throw;
                }
            }
        }

        private RowData<ColumnName, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="tjournal_detailid", ordinal=1 },
                new ExcelColumn(){ name="accountid", ordinal=6 },
            };

            return Utils.getDataFromExcel("Mapping Ganti Account Hutang di PCM - 2.xlsx", columns).ToArray();
        }
    }
}
